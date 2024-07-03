# Bitcoin to Postgres Parser

## Overview
This project is designed to parse Bitcoin blocks from `.dat` files and insert them into a PostgreSQL database. The code leverages asynchronous programming with `tokio` and database connection pooling with `bb8`.

## Features
- Sequential file processing using Tokio.
- Semaphore-based concurrency control for database insertion.
- Environmental configuration loading with dotenv.
- Connection pooling with bb8 and bb8_postgres.
- Graceful error handling and logging.

## Prerequisites
- Rust and Cargo installed.
- PostgreSQL database setup.
- Environment configuration.
- Bitcoin Core blocks directory /.bitcoin/blocks/

## Setup

### Clone the Repository
```bash
git clone https://github.com/michaelycrypto/bitcoin-postgres-parser.git
cd bitcoin-postgres-parser
```

### Install Dependencies
Ensure you have the necessary Rust toolchain installed. You can manage Rust installations using [rustup](https://rustup.rs/).
```sh
cargo build
```

### Configure Environment Variables
Create a `.env` file in the project root directory and add the following:
```
DATABASE_URL="host=localhost user=postgres password=postgres dbname=postgres"
BLOCKS_PATH=/home/bitcoind/.bitcoin/blocks/
```

### Database Schema Setup
The application will automatically set up the necessary database schema on the first run.

## Running the Application
```sh
cargo run
```

## Application Flow
- Initialize Environment: The application starts by loading environment variables from the .env file. This includes the DATABASE_URL for the PostgreSQL database and BLOCKS_PATH where the Bitcoin block files are located.
- Database Connection: Establishes a connection pool to the PostgreSQL database using bb8 and tokio_postgres. This allows for efficient management of database connections.
- Setup Database Schema: Calls the setup_database function to initialize the necessary database schema for storing Bitcoin blocks.
- Read Block Files: Reads the directory specified in BLOCKS_PATH and identifies all files that start with "blk" and have a ".dat" extension. These files are sorted to ensure blocks are processed in order.
- Process Blocks: For each file, the process_file function reads blocks and sends them to an asynchronous channel.
A separate task receives blocks from this channel and inserts them into the database. This task uses a semaphore to limit the number of concurrent insert operations, ensuring efficient and controlled resource usage.
- Monitor and Report: The application periodically reports progress, indicating how many blocks have been processed and the time taken for every 1000 blocks.

## Code Structure
- **main.rs**: Entry point of the application.
- **database.rs**: Handles database setup and block insertion logic.
- **models.rs**: Contains data models such as `Block`.
- **processing.rs**: Includes functions for processing block files.
- **utils.rs**: Utility functions used across the application.

## Environment Variables
- `DATABASE_URL`: PostgreSQL connection string.
- `BLOCKS_PATH`: Directory path where Bitcoin block files are stored.


## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Feel free to contribute to this project by opening issues or submitting pull requests. If you encounter any problems or have suggestions for improvements, please let us know.