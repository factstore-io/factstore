curl -i -X 'POST' \
  'http://localhost:8080/api/v1/stores/test/facts' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
   "facts":[
      {
         "type":"UserCreated",
         "subject":"user-4",
         "payload":{
            "data":"SGVsbG8gd29ybGQ="
         }
      }
   ]
}'
