# FactStore CLI

FactStore CLI is a command-line interface tool to interact with FactStore. 
It allows you to perform various operations such as appending facts, querying facts, and managing the FactStore instance.

## Usage Examples

```bash
factstore store create orders
factstore store list

factstore fact append orders order-placed '{"orderId": "12345", "amount": 100.0}'

factstore fact stream orders --from=beginning
```


## Developer Notes

Build the CLI using the following command:

```bash
./gradlew :factstore-cli:build -Dquarkus.native.enabled=true
```
This will generate a native executable in the `build` directory of the `factstore-cli` module. 

You can then run the CLI using the generated executable or define an alias for easier access. For example:

```bash
alias factstore=$(pwd)/factstore-cli/build/factstore-cli-1.0.0-SNAPSHOT-runner
```
