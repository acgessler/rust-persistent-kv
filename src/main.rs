#[allow(dead_code)]
use std::path::Path;
use std::process;
use clap::{ App, Arg, SubCommand };
#[allow(dead_code)]
mod snapshot_set;

use crate::snapshot_set::admin::SnapshotSetAdmin;

fn main() {
    let matches = App::new("SnapshotAdmin")
        .version("1.0")
        .author("Alexander Gessler")
        .about("Manages ongoing snapshots and backups produced by PersistentKeyValueStore")
        .subcommand(
            SubCommand::with_name("prune-backups")
                .about("Prunes backups")
                .arg(
                    Arg::with_name("path").help("The path to prune backups").required(true).index(1)
                )
                .arg(
                    Arg::with_name("max-backups")
                        .help("The maximum number of backups to keep")
                        .required(true)
                        .index(2)
                )
        )
        .subcommand(
            SubCommand::with_name("delete-failed-pending-snapshots")
                .about("Deletes failed pending snapshots")
                .arg(
                    Arg::with_name("path")
                        .help("The path to delete failed pending snapshots")
                        .required(true)
                        .index(1)
                )
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("prune-backups") {
        let mut snapshot_set = snapshot_set::FileSnapshotSet
            ::new(Path::new(matches.value_of("path").unwrap()))
            .unwrap();
        let max_backups = matches.value_of("max-backups").unwrap().parse::<usize>().unwrap();
        if let Err(e) = snapshot_set.prune_backup_snapshots(max_backups) {
            eprintln!("Error pruning backups: {}", e);
        }
    } else if let Some(matches) = matches.subcommand_matches("delete-failed-pending-snapshots") {
        let mut snapshot_set = snapshot_set::FileSnapshotSet
            ::new(Path::new(matches.value_of("path").unwrap()))
            .unwrap();
        if let Err(e) = snapshot_set.prune_not_completed_snapshots() {
            eprintln!("Error pruning failed pending snapshots: {}", e);
        }
    } else {
        eprintln!("No valid subcommand was used");
        process::exit(1);
    }
}
