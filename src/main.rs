use std::io;
use std::io::Write;
use std::path::Path;
use std::process;
use std::fs;
use std::path;
use std::io::prelude;
use std::env;

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

struct Row {
    id: i32,
    username: String,
    email: String
}

struct Page {
    max_rows: i32,
    num_rows: i32,
    rows: Vec<Box<Row>>
}

struct Pager {
    file_d: fs::File,
    file_len: i32,
    pages: Vec<Option<Box<Page>>>
}

struct Table {
    max_pages: i32,
    pager: Box<Pager>
}

fn pager_open(filename: &String) -> Box<Pager> {
    let filepath: &Path = Path::new(&filename);
    let mut file = match fs::File::open(filename) {
        Ok(file) => file,
        Err(err) => panic!("Couldnt open file: {}", filename)
    };

    let metadata  = match file.metadata() {
        Ok(mt) => mt,
        Err(err) => panic!("Could not read file metadata")
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
        max_pages: MAX_PAGES,
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
            if ele.rows.len() as i32  == ele.max_rows {

                if table.pager.pages.len() as i32 == table.max_pages {
                    return;
                }

                let page: Box<Page> = Box::<Page>::new(Page {max_rows: MAX_ROWS, num_rows: 1, rows: vec![row]});
                table.pager.pages.push(page);
            } else {
                ele.rows.push(row);
                ele.num_rows += 1;
            }
        },
        None => {

            if table.pager.pages.len() as i32 == table.max_pages {
                return;
            }
            
            let page: Box<Page> = Box::<Page>::new(Page {max_rows: MAX_ROWS, num_rows: 1, rows: vec![row]});
            table.pager.pages.push(page);
        }
    }
}

fn execute_select <'a> (id: i32, table: &'a Table) -> Result<&'a Box<Row>, String> {
    for page in table.pager.pages.iter() {
        for row in page.rows.iter() {
            if row.id as i32 == id {
                return Ok(row);
            }
        }
    }

    return Err(String::from("Could Not find the id"));
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
            Ok(ele) => {
                println!("Fetched the row --> id: {}, username: {}, email: {}", ele.id, ele.username, ele.email);
            },
            Err(e) => {
                print_error(&e.as_str());
            }
        };
        statement.statement_type = StatementType::StatementSelect;
        return PrepareResult::PrepareSuccess;
    } else {
        return PrepareResult::PrepareUnrecognizedStatement;
    }
}

fn get_page(pager: &Pager, page_num: i32) -> &Box<Page> {
    if page_num > MAX_PAGES {
        panic!("Tried to access page out of bounds");
    }

    match pager.pages[page_num as usize] {
        Some(page) => {
            return &page;
        },
        None => {
            
            let page: Box<Page> = Box::<Page>::new(Page {
                max_rows: MAX_ROWS,
                num_rows: 1,
                rows: 
            })
            void* page = malloc(PAGE_SIZE);
+    uint32_t num_pages = pager->file_length / PAGE_SIZE;
+
+    // We might save a partial page at the end of the file
+    if (pager->file_length % PAGE_SIZE) {
+      num_pages += 1;
+    }
+
+    if (page_num <= num_pages) {
+      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
+      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
+      if (bytes_read == -1) {
+        printf("Error reading file: %d\n", errno);
+        exit(EXIT_FAILURE);
+      }
+    }
+
+    pager->pages[page_num] = page;
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
