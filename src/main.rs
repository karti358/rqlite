use std::any::TypeId;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::path::Path;
use std::process;
use std::fs;
use std::path;
use std::io::prelude;
use std::env;
use std::mem;
use serde::{Serialize, Deserialize};

enum MetaCommandResult {
    MetaCommandSuccess,
    MetaCommandUnrecognizedCommand
}

enum PrepareResult {
    PrepareSuccess,
    PrepareUnrecognizedStatement
}

enum StatementType {
    Initial,
    StatementSelect,
    StatementInsert
}

struct Statement {
    statement_type: StatementType,
}

const MAX_TOTAL_ROWS: i32 = 1024;
const MAX_ROWS: i32 = 12;
const MAX_PAGES: i32 = MAX_TOTAL_ROWS / MAX_ROWS;
const PAGE_SIZE: i32 = 4096;

const ID_SIZE: i32 = mem::size_of::<i32>() as i32;
const USERNAME_SIZE: i32 = mem::size_of::<String>() as i32;
const EMAIL_SIZE: i32 = mem::size_of::<String>() as i32;
const ID_OFFSET: i32 = 0;
const USERNAME_OFFSET: i32 = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: i32 = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: i32 = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;


#[derive(Serialize, Deserialize, Debug)]
struct Row {
    id: i32,
    username: String,
    email: String
}

#[derive(Serialize, Deserialize, Debug)]
struct Page {
    rows: Vec<Box<Row>>
}

struct Pager {
    file_d: fs::File,
    file_len: i32,
    pages: Vec<Option<Box<Page>>>
}

struct Table {
    pager: Box<Pager>
}

fn pager_open(filename: &String) -> Box<Pager> {
    let filepath: &Path = Path::new(&filename);
    let mut file = match fs::File::open(filename) {
        Ok(file) => file,
        Err(_err) => panic!("Couldnt open file: {}", filename)
    };

    let metadata  = match file.metadata() {
        Ok(mt) => mt,
        Err(_err) => panic!("Could not read file metadata")
    };

    let mut pages = Vec::with_capacity(MAX_PAGES as usize);
    pages.resize_with(MAX_PAGES as usize, || None);

    let pager: Box<Pager> = Box::<Pager>::new(Pager {
        file_d: file,
        file_len: metadata.len() as i32,
        pages: pages
    });

    return pager
}

fn db_open(filename: &String) -> Table {
    let mut pager: Box<Pager> = pager_open(filename);

    let table: Table = Table {
        pager: pager
    };

    return table;
}


fn initial_prompt() -> io::Result<()> {
    print!("rsqlite>");
    io::stdout().flush()?;
    Ok(())
}

fn do_meta_command(meta_command: &str) -> MetaCommandResult {
    if meta_command == "exit" {
        println!("{}", meta_command);
        process::exit(0);
    } else {
        return MetaCommandResult::MetaCommandUnrecognizedCommand;
    }
}

fn execute_insert(row: Box<Row>, table: &mut Table) {
    match table.pager.pages.last_mut() {
        Some(ele) => {

            match ele {
                Some(ele2) => {
                    if ele2.rows.len() as i32 == MAX_ROWS {

                        if table.pager.pages.len() as i32 == MAX_PAGES{
                            return;
                        }

                        let page: Option<Box<Page>> = Option::Some(Box::<Page>::new(Page {
                            rows: vec![row]
                        }));

                        table.pager.pages.push(page);
                    } else {
                        ele2.rows.push(row);
                    }
                },
                None => {
                    if table.pager.pages.len() as i32 == MAX_PAGES {
                        return;
                    }
                    let page: Option<Box<Page>> = Option::Some(Box::<Page>::new(Page {
                        rows: vec![row]
                    }));
                    table.pager.pages[table.pager.pages.len() - 1] = page;
                }
            }
        },
        None => {
            if table.pager.pages.len() as i32 == MAX_PAGES {
                return;
            }
            let page: Option<Box<Page>> = Option::Some(Box::<Page>::new(Page {
                rows: vec![row]
            }));
            table.pager.pages.push(page);
        }
    }
}

fn execute_select <'a> (id: i32, table: &'a Table) -> Option<&Row> {
    for page in table.pager.pages.iter() {
        match page{
            Some(ele) => {
                for row in ele.rows.iter() {
                    if row.id as i32 == id {
                        return Some(row);
                    }
                }
            },
            None => {
                continue
            }
        }
    }

    return None;
} 

fn get_insert_values(input: &mut String) -> Box<Row> {
    let values: Vec<&str> = input.split(" ").collect();

    return Box::<Row>::new(Row {
        id: values[1].parse::<i32>().expect("Error in converting"),
        username: String::from(values[2]),
        email: String::from(values[3])
    })
}

fn get_id_select(input: &mut String) -> i32 {
    let values: Vec<&str> = input.split(" ").collect();
    return values[1].trim().parse::<i32>().expect("Could not parse id");
}

fn prepare_statement(input: &mut String, table: &mut Table, statement: &mut Statement) -> PrepareResult {
    if input.to_lowercase().contains("insert") {
        let row: Box<Row> = get_insert_values(input);
        execute_insert(row, table);
        statement.statement_type = StatementType::StatementInsert;
        return PrepareResult::PrepareSuccess;
    } else if input.to_lowercase().contains("select") {
        let id: i32 = get_id_select(input);
        match execute_select(id, table) {
            Some(ele) => {
                println!("Fetched the row --> id: {}, username: {}, email: {}", ele.id, ele.username, ele.email);
            },
            None => {
                println!("[]");
            }
        };
        statement.statement_type = StatementType::StatementSelect;
        return PrepareResult::PrepareSuccess;
    } else {
        return PrepareResult::PrepareUnrecognizedStatement;
    }
}

fn get_page(pager: &Pager, page_num: i32) -> Result<&Box<Page>> {
    if page_num > MAX_PAGES {
        panic!("Tried to access page out of bounds");
    }

    match pager.pages[page_num as usize] {
        Some(page) => {
            return Ok(&page);
        },
        None => {

            let num_pages: i32 = pager.file_len / PAGE_SIZE;
            if pager.file_len % PAGE_SIZE == 0 {
                num_pages += 1;
            }

            if page_num <= num_pages {
                pager.file_d.seek(io::SeekFrom::Start( (page_num * PAGE_SIZE) as u64));
                let mut buf  = [0 as u8; PAGE_SIZE as usize];
                let bytes_read = pager.file_d.read(&mut buf);
                match bytes_read {
                    Ok(n) => {
                        let row_string = str::from_utf8(&buf).expect("Error reading the page with page num");
                        let page = serde_json::from_str(&row_string).expect("Could not parse page");
                        match page {
                            Some(_ele) => {
                                pager.pages[page_num as usize] = page;
                                return Ok(&_ele);
                            },
                            None => {
                                return Err("Could not load page");
                            }
                        }
                    },
                    Err(_err) => {
                        print_error("Could not read the page");
                    }
                }
            } else {

            }
        }
    }
} 


fn print_error(err: &str) {
    println!("Exited due to error: {}", err);
}


fn main() {
    let args: Vec<String> = env::args().collect();

    if (args.len() as i32) < 1 {
        panic!("Please provide filename");
    }

    let mut input: String = String::new();
    let input_handler: io::Stdin = io::stdin();

    let mut table: Table = db_open(&args[1]);

    loop {
        let _ = initial_prompt();
        input.clear();

        match input_handler.read_line(&mut input) {
            Ok(_) => {

                if input.starts_with(".") {
                    match do_meta_command( (input.split_at(1).1).trim()) {
                        MetaCommandResult::MetaCommandSuccess => {
                            continue;
                        },
                        MetaCommandResult::MetaCommandUnrecognizedCommand => {
                            print_error(&"Unrecognised meta command");
                            process::exit(1);
                        }
                    }
                }

                let mut statement: Statement = Statement { statement_type: StatementType::Initial };
                match prepare_statement(&mut input, &mut table, &mut statement) {
                    PrepareResult::PrepareSuccess => {
                        continue;
                    },
                    PrepareResult::PrepareUnrecognizedStatement => {
                        print_error("Unrecognized statement");
                    }
                }
            },
            Err(error) => {
                print_error(&error.to_string());
                panic!("Could not read input");
            }
        };
    }
}


use serde::{Serialize, Deserialize};
use serde_json;

#[derive(Serialize, Deserialize, Debug)]
struct Person {
    name: String,
    age: u8,
    email: String,
}

fn main() {
    let person = Person {
        name: String::from("Murat"),
        age: 20,
        email: String::from("murat@example.com"),
    };

    // Serialize the struct to a JSON string
    let serialized = serde_json::to_string(&person).unwrap();
    println!("Serialized: {}", serialized);

    // Deserialize the JSON string back to a struct
    let deserialized: Person = serde_json::from_str(&serialized).unwrap();
    println!("Deserialized: {:?}", deserialized);
}