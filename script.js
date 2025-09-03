import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
    vus: 50, // virtual users
    duration: '10s', // test duration
    thresholds: {
        http_req_duration: ['p(90)<500'], // 90% of requests should be below 500ms
    },
};

export default function () {
    const res = http.post('http://localhost:8080/test');
    check(res, {
        'status is 204': (r) => r.status === 204,
    });
    // sleep(2)

    // sleep(10); // optional: simulate pacing
}

