# Load Testing


## Running the test

Run hardness in the docker container

```
docker run -it --rm -v ${PWD}:/build/source -e TEST_ADDRESS=localhost:8084  gvfn/clang-buildpack:ubuntu-10 bash
```

Once started run following commands

```
pip install pandas pycountry phonenumbers requests names asyncio websockets blessings 
```

Freeup ram on unbuntu

```
sync
sudo echo 3 > /proc/sys/vm/drop_caches
```