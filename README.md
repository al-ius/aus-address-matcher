## aus-address-matcher

> [!NOTE]  
> This repo is still a work in progress and should be treated like a proof of concept.

A simple address matcher for free form text inputs matching with addresses in the Australian GNAF database. 

### Get started

#### Setup:
```shell
python3 -m venv venv
. venv/bin/activate
python3 -m pip install -r requirements.txt
python3 src/initialise_db.py  # ~5-10 mins
```

#### Running:
Runs sample addresses from `src/test/data/sample_addresses.txt`
```shell
python3 src/address_matcher.py
```

Or to run a particular address
```shell
python3 src/address_matcher.py "245 HIGH STREET PRAHRAN VIC 3181"
```
