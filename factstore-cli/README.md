# FactStore CLI

FactStore CLI is a command-line interface tool to interact with FactStore. 
It allows you to perform various operations such as appending facts, querying facts, and managing the FactStore instance.

## Usage Examples

```bash
factstore store create orders
factstore store list

factstore fact append orders order-placed '{"orderId": "12345", "amount": 100.0}'

factstore fact stream orders --from=beginning

factstore find facts --store orders --subject order-1234

factstore find facts --store orders --since 5m # Find facts from the last 5 minutes

# find by tag
factstore find facts --store orders --tag key=value
factstore find facts --store orders --tag key=value --tag key2=value2

factstore store delete orders
```


## Developer Notes

Build the CLI using the following command:

```bash
./gradlew :factstore-cli:build -Dquarkus.native.enabled=true -Dquarkus.package.jar.enabled=false
```
This will generate a native executable in the `build` directory of the `factstore-cli` module. 

You can then run the CLI using the generated executable or define an alias for easier access. For example:

```bash
alias factstore=$(pwd)/factstore-cli/build/factstore-cli-1.0.0-SNAPSHOT-runner
```
