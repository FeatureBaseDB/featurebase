import http from 'k6/http';
import { sleep } from 'k6';
	
export default function () {
    const params = {
        timeout: '1800s',
    };

    let res = http.post(`http://${__ENV.DATANODE0}:10101/index/seg/query`, "GroupBy(Rows(education_level), Rows(gender), Rows(political_party), Rows(domain), aggregate=Sum(field=age))", params);
    sleep(1);
}
