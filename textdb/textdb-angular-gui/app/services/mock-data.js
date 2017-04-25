"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var keywordMatcher = {
    top: 20,
    left: 20,
    properties: {
        title: 'KeywordMatcher',
        inputs: {
            input_1: {
                label: 'Input (:i)',
            }
        },
        outputs: {
            output_1: {
                label: 'Output (:i)',
            }
        },
        attributes: {
            "operatorType": "KeywordMatcher",
            "query": "keyword",
            "attributes": ["attr1", "attr2"],
            "luceneAnalyzer": "standard",
            "matchingType": "conjunction",
            "addSpans": true
        }
    }
};
var regexMatcher = {
    top: 20,
    left: 20,
    properties: {
        title: 'RegexMatcher',
        inputs: {
            input_1: {
                label: 'Input(:i)',
            }
        },
        outputs: {
            output_1: {
                label: 'Output (:i)',
            }
        },
        attributes: {
            "operatorType": "RegexMatcher",
            "regex": "regex",
            "attributes": ["attr1", "attr2"],
            "regexIgnoreCase": false
        }
    }
};
var dictionaryMatcher = {
    top: 20,
    left: 20,
    properties: {
        title: 'DictionaryMatcher',
        inputs: {
            input_1: {
                label: "Input(:i)",
            }
        },
        outputs: {
            output_1: {
                label: "Output(:i)",
            }
        },
        attributes: {
            "operatorType": "DictionaryMatcher",
            "dictionaryEntries": ["entry1", "entry2"],
            "attributes": ["attr1", "attr2"],
            "luceneAnalyzer": "standard",
            "matchingType": "conjunction"
        }
    }
};
var fuzzyMatcher = {
    top: 20,
    left: 20,
    properties: {
        title: "FuzzyTokenMatcher",
        inputs: {
            input_1: {
                label: "Input(:i)",
            }
        },
        outputs: {
            output_1: {
                label: "Output(:i)",
            }
        },
        attributes: {
            "operatorType": "FuzzyTokenMatcher",
            "query": "token1 token2 token3",
            "attributes": ["attr1", "attr2"],
            "luceneAnalyzer": "standard",
            "thresholdRatio": 0.8
        }
    }
};
var nlpEntity = {
    top: 20,
    left: 20,
    properties: {
        title: 'NlpEntity',
        inputs: {
            input_1: {
                label: 'Input(:i)',
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "NlpEntity",
            "nlpEntityType": "location",
            "attributes": ["attr1", "attr2"]
        }
    }
};
var nlpSentiment = {
    top: 20,
    left: 20,
    properties: {
        title: 'NlpSentiment',
        inputs: {
            input_1: {
                label: 'Input(:i)',
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "NlpSentiment",
            "attribute": "inputAttr",
            "resultAttribute": "resultAttr"
        }
    }
};
var regexSplit = {
    top: 20,
    left: 20,
    properties: {
        title: 'RegexSplit',
        inputs: {
            input_1: {
                label: "Input (:i)",
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "RegexSplit",
            "splitRegex": "regex",
            "splitAttribute": "attr1",
            "splitType": "standalone"
        }
    }
};
var sampler = {
    top: 20,
    left: 20,
    properties: {
        title: 'Sampler',
        inputs: {
            input_1: {
                label: "Input (:i)",
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "Sampler",
            "sampleSize": 10,
            "sampleType": "firstk"
        }
    }
};
var projection = {
    top: 20,
    left: 20,
    properties: {
        title: 'Projection',
        inputs: {
            input_1: {
                label: "Input (:i)",
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "projection",
            "attributes": ["attr1", "attr2"]
        }
    }
};
var scanSource = {
    top: 20,
    left: 20,
    properties: {
        title: 'ScanSource',
        inputs: {
            input_1: {
                label: "Input (:i)",
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "ScanSource",
            "tableName": "promed"
        }
    }
};
var keywordSource = {
    top: 20,
    left: 20,
    properties: {
        title: 'KeywordSource',
        inputs: {
            input_1: {
                label: "Input (:i)",
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "KeywordSource",
            "query": "keyword",
            "attributes": ["attr1", "attr2"],
            "luceneAnalyzer": "standard",
            "matchingType": "conjunction",
            "tableName": "tableName",
            "addSpans": false
        }
    }
};
var dictionarySource = {
    top: 20,
    left: 20,
    properties: {
        title: 'DictionarySource',
        inputs: {
            input_1: {
                label: "Input (:i)",
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "dictionarySource",
            "dictionaryEntries": ["entry1", "entry2"],
            "attributes": ["attr1", "attr2"],
            "luceneAnalyzer": "standard",
            "matchingType": "conjunction",
            "tableName": "tableName"
        }
    }
};
var regexSource = {
    top: 20,
    left: 20,
    properties: {
        title: 'RegexSource',
        inputs: {
            input_1: {
                label: "Input (:i)",
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "RegexSource",
            "regex": "regex",
            "attributes": ["attr1", "attr2"],
            "regexIgnoreCase": false,
            "tableName": "tableName",
            "regexUseIndex": true
        }
    }
};
var fuzzyTokenSource = {
    top: 20,
    left: 20,
    properties: {
        title: 'FuzzyTokenSource',
        inputs: {
            input_1: {
                label: "Input (:i)",
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "FuzzyTokenSource",
            "query": "token1 token2 token3",
            "attributes": ["attr1", "attr2"],
            "luceneAnalyzer": "standard",
            "thresholdRatio": 0.8,
            "tableName": "tableName"
        }
    }
};
var characterDistanceJoin = {
    top: 20,
    left: 20,
    properties: {
        title: 'CharacterDistanceJoin',
        inputs: {
            input_1: {
                label: 'Input (:i)',
            },
            input_2: {
                label: "Input 2",
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "JoinDistance",
            "innerAttribute": "attr1",
            "outerAttribute": "attr1",
            "spanDistance": 100
        }
    }
};
var similarityJoin = {
    top: 20,
    left: 20,
    properties: {
        title: 'Similarity Join',
        inputs: {
            input_1: {
                label: 'Input (:i)',
            },
            input_2: {
                label: "Input 2",
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "SimilarityJoin",
            "innerAttribute": "attr1",
            "outerAttribute": "attr1",
            "similarityThreshold": 0.8
        }
    }
};
var compareNumber = {
    top: 20,
    left: 20,
    properties: {
        title: 'CompareNumber',
        inputs: {
            input_1: {
                label: "Input (:i)",
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "CompareNumber",
            "attribute": "attribute",
            "compareNumber": "=",
            "threshold": 0,
        }
    }
};
var aggregation = {
    top: 20,
    left: 20,
    properties: {
        title: 'Aggregation',
        inputs: {
            input_1: {
                label: "Input (:i)",
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "Aggregation",
            "attribute": "attribute",
            "aggregationType": "count",
            "resultAttribute": "result attribute",
        }
    }
};
var result = {
    top: 20,
    left: 20,
    properties: {
        title: 'View Results',
        inputs: {
            input_1: {
                label: "Input (:i)",
            }
        },
        outputs: {
            output_1: {
                label: "Output (:i)",
            }
        },
        attributes: {
            "operatorType": "ViewResults",
            "limit": 2147483647,
            "offset": 0
        }
    }
};
var jsonSink = {
    top: 20,
    left: 20,
    properties: {
        title: 'Save as Json',
        inputs: {
            input_1: {
                label: "Input (:i)",
            }
        },
        outputs: {},
        attributes: {
            "operatorType": "JsonSink",
            "filePath": "jsonFilePath"
        }
    }
};
exports.DEFAULT_MATCHERS = [
    { id: 0, jsonData: regexMatcher },
    { id: 1, jsonData: keywordMatcher },
    { id: 2, jsonData: dictionaryMatcher },
    { id: 3, jsonData: fuzzyMatcher },
    { id: 4, jsonData: nlpEntity },
    { id: 5, jsonData: nlpSentiment },
    { id: 6, jsonData: regexSplit },
    { id: 7, jsonData: sampler },
    { id: 8, jsonData: projection },
    { id: 9, jsonData: scanSource },
    { id: 10, jsonData: keywordSource },
    { id: 11, jsonData: dictionarySource },
    { id: 12, jsonData: regexSource },
    { id: 13, jsonData: fuzzyTokenSource },
    { id: 14, jsonData: characterDistanceJoin },
    { id: 15, jsonData: similarityJoin },
    { id: 16, jsonData: compareNumber },
    { id: 17, jsonData: aggregation },
    { id: 18, jsonData: jsonSink },
    { id: 19, jsonData: result },
];
//# sourceMappingURL=mock-data.js.map