# GH2EH
A python docker service for pulling github data into eventhub.

## Run Instructions 

To run call the following docker command

```
docker run --rm -it  \
                    -e eh_address=" EventHub Address in format 'amqps://<mynamespace>.servicebus.windows.net/myeventhub'"\
                    -e eh_user="EventHub User"\
                    -e eh_key="EventHub Key"
                    -e gh_token="Github developer token" \
                    abornst/gh2eh
'''

