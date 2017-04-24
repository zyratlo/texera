import { Injectable } from '@angular/core';

import { Data } from './data';
import { DEFAULT_MATCHERS } from './mock-data';

@Injectable()
export class MockDataService {

    getMatchers(): Promise<Data[]> {
        return Promise.resolve(DEFAULT_MATCHERS);
    }

}
