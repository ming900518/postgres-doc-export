use indexmap::IndexMap;
use postgres::{Client, NoTls};
use std::env;
use std::ops::Add;
use std::process::exit;
use std::time::{SystemTime, UNIX_EPOCH};
use xlsxwriter::*;

const SQL: &str = r#"
select coalesce(c.table_name, '')    as table_name,
       coalesce(c.column_name, '')   as column_name,
       coalesce(c.is_nullable, '')   as is_nullable,
       coalesce(c.data_type, '')     as data_type,
       coalesce(pgd.description, '') as description
from pg_catalog.pg_statio_all_tables as st
         full outer join pg_catalog.pg_description pgd on pgd.objoid = st.relid
         full outer join information_schema.columns c on (
            pgd.objsubid = c.ordinal_position and
            c.table_schema = st.schemaname and
            c.table_name = st.relname
    )
where c.table_schema = $1
order by table_name, c.ordinal_position;
"#;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        exit(0)
    } else {
        let mut client = Client::connect(args[1].as_str(), NoTls).unwrap();
        let mut map: IndexMap<&str, Vec<Vec<&str>>> = IndexMap::new();
        match client.query(SQL, &[&args[2]]) {
            Ok(result) => {
                let mut rows: Vec<Vec<&str>> = Vec::new();
                result.iter().for_each(|row| {
                    let table_name: &str = row.get("table_name");
                    if !map.contains_key(table_name) {
                        rows = Vec::new();
                    }
                    let column_name: &str = row.get("column_name");
                    let is_nullable: &str = row.get("is_nullable");
                    let data_type: &str = row.get("data_type");
                    let description: &str = row.get("description");
                    rows.push(vec![column_name, is_nullable, data_type, description]);
                    map.insert(table_name, rows.clone());
                });
                let time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                let filename = format!("output-{:?}.xlsx", time);
                let workbook = Workbook::new(filename.as_str());
                map.iter().for_each(|(index, value)| {
                    if let Ok(mut worksheet) = workbook.add_worksheet(Some(index)) {
                        worksheet
                            .write_string(0, 0, "欄位名稱", None)
                            .expect("Failed to write result to output.xlsx");
                        worksheet
                            .write_string(0, 1, "是否可為空", None)
                            .expect("Failed to write result to output.xlsx");
                        worksheet
                            .write_string(0, 2, "型別", None)
                            .expect("Failed to write result to output.xlsx");
                        worksheet
                            .write_string(0, 3, "註釋", None)
                            .expect("Failed to write result to output.xlsx");
                        value.iter().enumerate().for_each(|(row_count, row)| {
                            row.iter().enumerate().for_each(|(column_count, column)| {
                                worksheet
                                    .write_string(
                                        row_count.add(1) as WorksheetRow,
                                        column_count as WorksheetCol,
                                        column,
                                        None,
                                    )
                                    .expect("Failed to write result to output.xlsx");
                            });
                        })
                    }
                });
                workbook.close().unwrap();
                println!(
                    "Successfully generated documentation for {} with filename {}.",
                    args[2], filename
                );
            }
            Err(e) => {
                println!("Failed, reason: {}", e);
                exit(1)
            }
        }
    }
}
