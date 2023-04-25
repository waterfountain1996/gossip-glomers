# Gossip Glomers

A series of distributed systems challenges by [Fly.io](https://fly.io/).

Read more [here](https://fly.io/dist-sys/).


## Challenges

Make sure that `maelstrom` is in your `$PATH`.

### 1 - Echo

```bash
maelstrom test -w echo --bin ./challenges/1-echo.py --node-count 1 --time-limit 10
```

### 2 - Unique ID generation

```bash
maelstrom test -w unique-ids --bin ./challenges/2-unique-ids.py \
  --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```

### 3a - Broadcast

```bash
maelstrom test -w broadcast --bin ./challenges/3-broadcast.py \
  --node-count 1 --time-limit 20 --rate 10
```

### 3b - Multi-node broadcast

```bash
maelstrom test -w broadcast --bin ./challenges/3-broadcast.py \
  --node-count 5 --time-limit 20 --rate 10
```

### 3c - Fault tolerant broadcast

```bash
maelstrom test -w broadcast --bin ./challenges/3-broadcast.py \
  --node-count 5 --time-limit 20 --rate 10 --nemesis partition
```


### 4 - Grow-only counter

```bash
maelstrom test -w g-counter --bin ./challenges/4-grow-only-counter.py \
  --node-count 3 --rate 100 --time-limit 20 --nemesis partition
```
