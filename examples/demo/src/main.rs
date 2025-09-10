use colored::Colorize;
use std::{
    io::{Write, stdin, stdout},
    process::{Command, Stdio, exit},
};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!();
    println!("  🐕 Welcome! This is a demo of PgDog.");
    println!();
    println!("  PgDog is a pooler and proxy for sharding PostgreSQL.");
    println!(
        "  This demo is interactive. You'll be running examples \n  and using PgDog directly."
    );
    println!("\n  Let's get started! When you're ready, pretty any key.\n");
    input();
    demo(0);
}

fn info(what: impl ToString) {
    let mut counter = 0;
    let what = what.to_string();
    print!("  ");
    for c in what.chars() {
        if counter > 60 {
            if c == ' ' {
                print!("\n  ");
                counter = 0;
            } else {
                print!("{c}");
            }
        } else {
            print!("{c}");
        }

        counter += 1;
    }
    println!();
    stdout().flush().unwrap();
}

fn input() -> String {
    print!("\n > ");
    stdout().flush().unwrap();
    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();

    match input.as_str().trim() {
        "exit" | "quit" => exit(0),
        _ => (),
    };
    println!();
    input
}

fn command(info: &str, cmd: &mut Command) -> bool {
    print!("{info}");
    stdout().flush().unwrap();

    let status = cmd.status().unwrap();

    if status.success() {
        println!("✅");
        true
    } else {
        println!("❌");
        false
    }
}

fn toml_number(name: &str, value: &str) {
    println!("  {} {} {}", name.cyan(), "=".yellow(), value.purple());
}

fn toml_string(name: &str, value: &str) {
    println!(
        "  {} {} {}",
        name.cyan(),
        "=".yellow(),
        "\"".green().to_string() + &format!("{}", value.green()) + &"\"".green().to_string()
    );
}

fn config_shard(port: usize, shard: usize) {
    println!("  {}", "[[databases]]".cyan());
    toml_string("name", "postgres");
    toml_string("host", "127.0.0.1");
    toml_number("port", port.to_string().as_str());
    toml_number("shard", shard.to_string().as_str());
}

fn config() {
    println!("  {}  ", "pgdog.toml".bold().italic());
    println!();
    config_shard(6000, 0);
    println!();
    config_shard(6001, 1);
    println!();
    config_shard(6002, 2);
    println!();
}

fn check(what: &str) -> bool {
    print!("  checking for {what}...");

    let ok = Command::new(what)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success();

    if ok {
        println!("✅");
        true
    } else {
        println!(
            "❌\n\n  {what} isn't installed! Please install it before proceeding.",
        );
        false
    }
}

fn demo(step: usize) -> bool {
    match step {
        0 => {
            println!(
                "\n  First things first, let's check if you have\n  the necessary dependencies.\n"
            );

            if !check("docker-compose") {
                return false;
            }
            if !check("docker") {
                return false;
            }

            return demo(1);
        }

        1 => {
            println!();
            info(
                "Good to go. First things first, I'm going to create 3 PostgreSQL databases with Docker. \
They are going to run on ports 6000, 6001, and 6002. Press any key when you're ready.",
            );
            info("");
            input();
            return demo(3);
        }

        2 => {
            println!();
            command(
                "  Starting PostgreSQL, give me a second...",
                Command::new("docker-compose")
                    .arg("up")
                    .arg("-d")
                    .stderr(Stdio::null())
                    .stdout(Stdio::null()),
            );

            return demo(3);
        }

        3 => {
            println!();
            info("Great. PostgreSQL is running, let's configure PgDog to connect to it.");
            println!();
            info("PgDog has two configuration files:\n");
            info("  - pgdog.toml");
            info("  - users.toml");
            println!();

            info("pgdog.toml".bold().to_string() +
                " is used for configuring database connections while " + &"users.toml".bold().to_string() + " stores usernames and passwords. \
This is done so you can encrypt users.toml in prod, while being able to see your other settings in plain text.
                ",
            );
            info(
                "Let's configure ".to_string()
                    + &"pgdog.toml".bold().to_string()
                    + " first. Since we have 3 shards on ports 6000, 6001, and 6002 respectively, let's add 3 databases to the config.",
            );

            input();

            config();

            input();

            info(
                "Great. In the config, we have 3 databases. They all have the same name, \"postgres\", but are \
identified with 3 different shard numbers. This is how PgDog knows that the 3 DBs are part of the same \
sharded Postgres cluster.",
            );
        }

        n => {
            println!("Step {n} doesn't exist. Try again?");
        }
    }

    true
}
