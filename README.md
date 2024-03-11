# Block reader template

Allows to backfill data based on the range of blocks

## To run

Create `.env` file and fill details:
```
FROM_BLOCK=111000000
TO_BLOCK=111100000
BLOCKS_DATA_PATH=/data/blocks
```

Then run the following command:

```bash
cargo run --release --bin block-reader
```
