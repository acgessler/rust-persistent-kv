use std::env;
#[allow(dead_code)]
use std::path::Path;
use std::process;
#[allow(dead_code)]
mod snapshot_set;

use crate::snapshot_set::admin::SnapshotSetAdmin;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("No valid subcommand was used");
        process::exit(1);
    }

    match args[1].as_str() {
        "prune-backups" => {
            if args.len() != 4 {
                eprintln!("Usage: {} prune-backups <path> <max-backups>", args[0]);
                process::exit(1);
            }
            let path = &args[2];
            let max_backups = args[3].parse::<usize>().unwrap_or_else(|_| {
                eprintln!("Invalid number for max-backups");
                process::exit(1);
            });

            let mut snapshot_set = snapshot_set::FileSnapshotSet::new(Path::new(path)).unwrap();
            if let Err(e) = snapshot_set.prune_backup_snapshots(max_backups) {
                eprintln!("Error pruning backups: {}", e);
            }
        }
        "delete-failed-pending-snapshots" => {
            if args.len() != 3 {
                eprintln!("Usage: {} delete-failed-pending-snapshots <path>", args[0]);
                process::exit(1);
            }
            let path = &args[2];

            let mut snapshot_set = snapshot_set::FileSnapshotSet::new(Path::new(path)).unwrap();
            if let Err(e) = snapshot_set.prune_not_completed_snapshots() {
                eprintln!("Error pruning failed pending snapshots: {}", e);
            }
        }
        _ => {
            eprintln!("No valid subcommand was used");
            process::exit(1);
        }
    }
}
