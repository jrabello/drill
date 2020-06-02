use std::path::Path;
use std::{process, collections::HashMap};

use async_trait::async_trait;
use yaml_rust::Yaml;

use crate::actions::{extract_optional, Request, Runnable};
use crate::benchmark::{Context, Pool, Reports};
use crate::config::Config;
use crate::reader;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

#[derive(Clone)]
pub struct OneCsvLine {
  pub idx: usize,
  pub csv_rows_size: usize,
  pub csv_line: Option<Yaml>,
  pub assigned_var_key: Option<String>,
}

impl OneCsvLine {
  pub fn new(idx: usize, csv_rows_size: usize, item: &Yaml, _with_item: Option<Yaml>) -> OneCsvLine {
    let assign = extract_optional(item, "assign");

    OneCsvLine {
      idx,
      csv_rows_size,
      assigned_var_key: assign.map(str::to_string),
      csv_line: _with_item,
    }
  }
}

#[async_trait]
impl Runnable for OneCsvLine {
  async fn execute(&self, context: &mut Context, _reports: &mut Reports, _pool: &mut Pool, _config: &Config) {
    if self.csv_line.is_none() {
      return;
    }

    if let Some(ref assigned_key) = self.assigned_var_key {
        let json = yaml_to_json(self.csv_line.clone().unwrap());
        let json_value = json["txn"].clone();
        let concurrency = context.get(&"concurrency".to_owned());
        // println!("{:?}", json_value);

        if let Some(conc) = concurrency {
          let conc_num = conc.as_str().unwrap().parse::<usize>().unwrap();
          if conc_num > self.csv_rows_size {
            println!("Too many vusers:{} for data_size: {}", conc_num, self.csv_rows_size);
            process::exit(1);
          }

          if conc_num == self.idx {
            let body: Value = serde_json::from_str(json_value.as_str().unwrap()).unwrap_or(serde_json::Value::Null);
            println!("body: {:?}", body);

            context.insert(assigned_key.to_owned(), body);
          }
        }
    }
  }
}

pub fn yaml_to_json(data: Yaml) -> Value {
  if let Some(b) = data.as_bool() {
    json!(b)
  } else if let Some(i) = data.as_i64() {
    json!(i)
  } else if let Some(s) = data.as_str() {
    json!(s)
  } else if let Some(h) = data.as_hash() {
    let mut map = Map::new();

    for (key, value) in h.iter() {
      map.entry(key.as_str().unwrap()).or_insert(yaml_to_json(value.clone()));
    }

    json!(map)
  } else if let Some(v) = data.as_vec() {
    let mut array = Vec::new();

    for value in v.iter() {
      array.push(yaml_to_json(value.clone()));
    }

    json!(array)
  } else {
    panic!("Unknown Yaml node")
  }
}

pub fn is_that_you(item: &Yaml) -> bool {
  // println!("is_that_you with_one_item_from_csv: {:?}", item);
  // item["request"].as_hash().is_some() &&
  item["with_one_item_from_csv"].as_str().is_some() || item["with_one_item_from_csv"].as_hash().is_some()
}

pub fn expand(parent_path: &str, item: &Yaml, list: &mut Vec<Box<(dyn Runnable + Sync + Send)>>) {
  let (with_items_path, quote_char, csv_row) = if let Some(with_items_path) = item["with_one_item_from_csv"].as_str() {
    (with_items_path, b'\"', 0)
  } else if let Some(_with_items_hash) = item["with_one_item_from_csv"].as_hash() {
    let with_items_path = item["with_one_item_from_csv"]["file_name"].as_str().expect("Expected a file_name");
    let quote_char = item["with_one_item_from_csv"]["quote_char"].as_str().unwrap_or("\"").bytes().next().unwrap();
    let csv_row: i64 = item["with_one_item_from_csv"]["csv-row-to-assign"].as_i64().unwrap_or(0);

    (with_items_path, quote_char, csv_row)
  } else {
    panic!("WAT"); // Impossible case
  };

  let with_items_filepath = Path::new(parent_path).with_file_name(with_items_path);
  let final_path = with_items_filepath.to_str().unwrap();

  let with_items_file = reader::read_csv_file_as_yml(final_path, quote_char);
  // let with_item = with_items_file.get(csv_row as usize);
  // let csv_line = with_item.unwrap().to_owned();
  // dbg!(item_unwraped);
  
  for (i, with_item) in with_items_file.iter().enumerate() {
    // println!("quote_char: {} item: {:?} csv_line: {:?}", quote_char, item, csv_line);
    let csv_line = with_item.to_owned();
    list.push(Box::new(OneCsvLine::new(i, with_items_file.len(),item, Some(csv_line))));
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::actions::Runnable;

  #[test]
  fn expand_multi() {
    let text = "---\nname: foobar\nrequest:\n  url: /api/{{ item.id }}\nwith_one_item_from_csv: example/fixtures/users.csv";
    let docs = yaml_rust::YamlLoader::load_from_str(text).unwrap();
    let doc = &docs[0];
    let mut list: Vec<Box<(dyn Runnable + Sync + Send)>> = Vec::new();

    expand("./", &doc, &mut list);

    assert_eq!(is_that_you(&doc), true);
    assert_eq!(list.len(), 2);
  }
}
