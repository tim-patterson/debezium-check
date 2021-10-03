use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use serde::Deserialize;
use std::borrow::Cow;
use std::collections::HashSet;
use std::convert::TryInto;
use std::error::Error;
use std::time::{Duration, Instant};

use clap::Clap;

#[derive(Clap)]
struct Opts {
    #[clap(short, long, default_value = "localhost:9092")]
    broker: String,
    #[clap(short, long)]
    topic: String,
    #[clap(short, long)]
    partition: Option<i32>,
    #[clap(short, long)]
    config: Vec<String>
}

fn main() -> Result<(), Box<dyn Error>> {
    let opts: Opts = Opts::parse();
    let broker = &opts.broker;
    let topic = &opts.topic;
    let partition = &opts.partition;
    let report_freq = Duration::from_secs(30);

    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", broker)
        .set("group.id", "debezium_check");

    for pair in &opts.config {
        if let Some((key, value)) = pair.split_once("=") {
            config.set(key, value);
        } else {
            return Err(Box::from(format!("configs must be key value pairs in the form --config k=v, got {}", pair)));
        }
    }

    let consumer = BaseConsumer::from_config(&config)?;
    let mut topic_partition_list = TopicPartitionList::new();
    if let Some(partition) = partition {
        topic_partition_list.add_partition(topic, *partition);
    } else {
        let metadata = consumer.fetch_metadata(Some(topic), None)?;
        for partition in metadata.topics()[0].partitions() {
            topic_partition_list.add_partition(topic, partition.id());
        }
    }
    topic_partition_list.set_all_offsets(Offset::Beginning)?;
    consumer.assign(&topic_partition_list)?;

    // Set of "live" documents
    // Initial size is 1.2gb
    let mut live_docs: HashSet<[u8; 12]> = HashSet::with_capacity(100_000_000);

    // Missing docs, the docs we receive updates for but that we can't find
    let mut missing_docs: HashSet<[u8; 12]> = HashSet::new();

    let mut last_report = Instant::now();
    let mut creates = 0;
    let mut updates = 0;
    let mut deletes = 0;

    loop {
        let data = consumer.poll(Duration::from_millis(100));

        if let Some(message_result) = data {
            let message = message_result?;
            let oplog_key: OplogKey = serde_json::de::from_slice(message.key().unwrap())?;
            let object_id_hex = serde_json::de::from_str::<ObjectId>(&oplog_key.id)?.oid;
            let object_id: [u8; 12] = hex::decode(&*object_id_hex)?.as_slice().try_into()?;

            let oplog_value: OplogValue = serde_json::from_slice(message.payload().unwrap())?;

            match &*oplog_value.op {
                "c" | "r" => {
                    live_docs.insert(object_id);
                    creates += 1;
                }
                "d" => {
                    if !live_docs.remove(&object_id) {
                        println!("Delete for non-existent document {}", object_id_hex);
                    }
                    missing_docs.remove(&object_id);
                    deletes += 1;
                }
                "u" => {
                    if !live_docs.contains(&object_id) {
                        println!("Document {} not found", object_id_hex);
                        missing_docs.insert(object_id);
                    }
                    updates += 1;
                }
                _ => panic!("Op {} unknown", oplog_value.op),
            }
        }

        if last_report.elapsed() > report_freq {
            println!("Collection size is {}", live_docs.len());
            println!("Missing docs size is {}", missing_docs.len());
            println!("Processed since last report:");
            println!("  creates: {}", creates);
            println!("  updates: {}", updates);
            println!("  deletes: {}", deletes);
            last_report = Instant::now();
            creates = 0;
            updates = 0;
            deletes = 0;
        }
    }
}

#[derive(Deserialize)]
struct OplogValue<'a> {
    op: Cow<'a, str>,
}

#[derive(Deserialize)]
struct OplogKey {
    id: String,
}

#[derive(Deserialize)]
struct ObjectId<'a> {
    #[serde(alias = "$oid")]
    oid: Cow<'a, str>,
}
