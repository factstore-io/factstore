import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
    vus: 40, // virtual users
    duration: '30s', // test duration
    thresholds: {
        http_req_duration: ['p(90)<500'], // 90% of requests should be below 500ms
    },
};

export default function () {
    const res = http.post('http://localhost:8080/test');
    check(res, {
        'status is 200': (r) => r.status === 200,
    });
   //  sleep(1)
    // const res2 = http.get('http://localhost:8080/test');
   //  check(res2, {
     //    'status is 200': (r) => r.status === 200,
     //});

    // sleep(10); // optional: simulate pacing
}

