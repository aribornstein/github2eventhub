FROM python:3.6-alpine

ENV eh_address='to be defined by user'
ENV eh_user='to be defined by user'
ENV eh_key='to be defined by user'
ENV gh_endpoint='https://api.github.com/events?per_page=100'
ENV gh_token='to be defined by user'

RUN apk add --no-cache openssl-dev && \
    apk add --no-cache --virtual .build-deps gcc cmake openssl-dev build-base libffi-dev && \
    python3 -m pip install requests azure-eventhub --no-cache-dir &&\
    apk --purge del .build-deps

COPY ./gh2eh.py /home/gh2eh.py

CMD python ./home/gh2eh.py --eh_address=$eh_address --eh_user=$eh_user \
                           --eh_key=$eh_key --gh_endpoint=$gh_endpoint --gh_token=$gh_token 

