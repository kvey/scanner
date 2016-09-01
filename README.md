## Usage:

```
Add accounts to accounts.txt in accounts_and_proxies
Add proxy list to proxies.txt in accounts_and_proxies

Add approriate shapefiles to geodata
Change spawn-allocator.py geodata file references appropriately

$ python ./spawn-allocator.py 
to generate queryplan based on accounts/proxies/geodata

$ go build
$ ./scanner
to run the queryplan

changing populate_scan_environment to populate_scan_environment_test_proxies will build a proxy test plan instead
change worker to workerProxyTest in scanner and rebuild + run to verify proxies instead

```
