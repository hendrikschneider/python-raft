# python-raft

# Create virtual env
virtualenv --no-site-packages venv -p python3

# Activate
source venv/bin/activate

# Install dependicies
pip install websockets

# Run
python draft_client.py --listen 127.0.0.1:8009


# Run multiple servers
Open three consoles, activate the venv and run the client on a different port

### Terminal one
python draft_client.py --listen 127.0.0.1:8007

### Terminal two
python draft_client.py --listen 127.0.0.1:8008

### Terminal three
python draft_client.py --listen 127.0.0.1:8009
