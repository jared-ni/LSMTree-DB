CS265 LSM Tree Project

How to run server/clients, while in the base directory:

```bash
cd src
```

```bash
make clean
```

```bash
make
```

On terminal A: 
```bash
./server
```

On terminal B/C/D, etc.:
```bash
./client
```

Available APIs from the client terminal: get (g <key>), put (p <key> <val>), range (r <min-key> <max-key>), load (l "<file-location>"), print stats (s).

To run tests for profiling:
```bash
cd experiments/
```
Then, cd into any of the get/, range/, put/, range/, etc.

Then, run the <operation>_benchmark.sh script (Usage: <operation>_benchmark.sh <data-size>). 
Example data size: 10MB, 100MB, 1GB, 10GB, etc. Make sure the generator generates the <data-size>_<operation>.txt file in the folder first.

The output of the benchmark script will be sent to a folder named from hyperparameter configs, and this needs to be configured in the shell script before the program can be run. 









