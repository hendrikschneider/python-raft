# Python-raft
Please follow the following steps to install the requirements and start five raft instances.

# Create virtual env
```virtualenv --no-site-packages -p python3 venv```

# Activate
```source venv/bin/activate```

# Install dependencies
```pip install websockets```

# Run 
Usually, a raft network consists of five nodes. Open five consoles, activate the virtual environment and run each instance on a diffrent port.
Five servers running on 127.0.0.1 are preconfigured:
- Node 1: 127.0.0.1:8007
- Node 2: 127.0.0.1:8008
- Node 3: 127.0.0.1:8009
- Node 4: 127.0.0.1:8010
- Node 5: 127.0.0.1:8011 

##### Terminal one
```python raft.py --listen 127.0.0.1:8007```

##### Terminal two
```python raft.py --listen 127.0.0.1:8008```

##### Terminal three
```python raft.py --listen 127.0.0.1:8009```

##### Terminal three
```python raft.py --listen 127.0.0.1:8010```

##### Terminal three
```python raft.py --listen 127.0.0.1:8011```
