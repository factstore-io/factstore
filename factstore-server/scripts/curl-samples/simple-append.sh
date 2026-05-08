curl -X 'POST' \
  'http://localhost:8080/api/v1/stores/test55/facts' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
   "facts":[
      {
         "type":"UserCreated",
         "subjectRef":{
            "type":"user",
            "id":"user-3"
         },
         "payload":{
            "data":"SGVsbG8gd29ybGQ="
         },
         "metadata":{

         },
         "tags":{
            "vu":"bd9095f2-5494-4283-8511-9cacd3612a90"
         }
      }
   ]
}'
