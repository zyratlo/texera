import { Injectable } from '@angular/core';

import { Data } from './data';
import { DEFAULT_MATCHERS } from './mock-data';

@Injectable()
export class MockDataService {

    getOperatorList(): Promise<Data[]> {
        return Promise.resolve(DEFAULT_MATCHERS);
    }

    findOperatorData(operatorId: number): any {
        let operatorList = DEFAULT_MATCHERS;
        for (let operator of operatorList) {
            if (operator.id === operatorId) {
                return operator.jsonData;
            }
        }
        return null;
    }

}
