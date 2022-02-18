import http from 'k6/http';
import { sleep } from 'k6';
	
export default function () {
    http.get(`https://${__ENV.DATANODE0}`);
    sleep(1);
}