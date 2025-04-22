## Data Fabric Client

This repository contains a client that enables the interaction with the Knowledge Graph (Data Catalog) as well as with Connectors within the Data Fabric Framework.

### starting jupyter lab command:

```bash
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser
```

### Using tmux for permanent jupyterlab

- creating a new permanent tmux terminal called jupyterlab:
```commandline
tmux new -s jupyterlab 
```
Inside, jupyterlab may be started and run permanently. After starting jupyterlab inside, press Ctrl + B, then D for exiting the tmux terminal.

---
- listing all sessions: 
```commandline
tmux ls 
```

- checking out the session:
```commandline
tmux attach -t jupyterlab
```

- killing the session:
```commandline
tmux kill-session -t jupyterlab
```
