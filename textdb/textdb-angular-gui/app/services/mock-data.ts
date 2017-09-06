import { Data } from './data';

let keywordMatcher = {
    top: 20,
    left: 20,
    properties: {
        title: 'Keyword Search',
        inputs: {
            input_1: {
                label: '',
            }
        },
        outputs: {
            output_1: {
                label: '',
            }
        },
        attributes: {
            "operatorType": "KeywordMatcher",
            "attributes": [],
            "query": "keyword",
            "luceneAnalyzer": "standard",
            "matchingType": "phrase",
            "spanListName": " "
        },
        image : '../thirdparty/images/keywordSearch.png',
        color : '#80bfff',
    }
};

let regexMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : 'Regex Match',
    inputs : {
      input_1 : {
        label : '',
      }
    },
    outputs : {
      output_1 : {
        label : '',
      }
    },
    attributes : {
        "operatorType": "RegexMatcher",
        "attributes": [],
        "regex": "regex",
        "regexIgnoreCase": false,
        "spanListName": " "
    },
    image : '../thirdparty/images/regexMatch.png',
    color : '#66b3ff',
  }
};

let dictionaryMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : 'Dictionary Search',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs :{
      output_1 : {
        label : "",
      }
    },
    attributes :  {
        "operatorType": "DictionaryMatcher",
        "attributes": [],
        "dictionaryEntries": [],
        "luceneAnalyzer": "standard",
        "matchingType": "phrase",
        "spanListName": " "
    },
    image : '../thirdparty/images/dictionarySearch.png',
    color : '#66b3ff',
  }
}

let fuzzyMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : "Fuzzy Token Match",
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "FuzzyTokenMatcher",
        "attributes": [],
        "query": "token1 token2 token3",
        "luceneAnalyzer": "standard",
        "thresholdRatio": 0.8,
        "spanListName": " ",
    },
    image : '../thirdparty/images/fuzzy.png',
    color : '#66b3ff',
  }
}

let nlpEntity = {
  top : 20,
  left : 20,
  properties : {
    title : 'Entity recognition',
    inputs : {
      input_1 : {
        label : '',
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "NlpEntity",
        "attributes": [],
        "nlpEntityType": "location",
        "spanListName": " "
    },
    image : '../thirdparty/images/entityRecognition.png',
    color : '#85e085',
  }
}

let nlpSentiment = {
  top : 20,
  left : 20,
  properties : {
    title : 'Sentiment Analysis',
    inputs : {
      input_1 : {
        label : '',
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "NlpSentiment",
        "attribute": "",
        "resultAttribute": "resultAttr"
    },
    image : '../thirdparty/images/sentimentAnalysis.png',
    color : '#85e085',
  }
}

let emojiSentiment = {
  top : 20,
  left : 20,
  properties : {
    title : 'Emoji Sentiment Analysis',
    inputs : {
      input_1 : {
        label : '',
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "EmojiSentiment",
        "attribute": "",
        "resultAttribute": "resultAttr"
    },
    image : '../thirdparty/images/emojiAnalysis.png',
    color : '#85e085',
  }
}

let nltkSentiment = {
  top : 20,
  left : 20,
  properties : {
    title : 'Nltk Sentiment Analysis',
    inputs : {
      input_1 : {
        label : '',
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "NltkSentiment",
        "attribute": "",
        "resultAttribute": "resultAttr",
        "batchSize": "1000",
        "inputAttributeModel": "NltkSentiment.pickle"
    },
    image : "../thirdparty/images/nltk.jpg",
    color : '#85e085',
  }
}

let regexSplit = {
  top : 20,
  left : 20,
  properties : {
    title : 'Regex Split',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "RegexSplit",
        "attribute": "",
        "splitRegex": "regex",
        "resultAttribute": "splitText",
        "splitType": "standalone",
        "splitOption": "oneToMany"
    },
    image : '../thirdparty/images/regex.png',
    color : '#e6e600',
  }
}

let nlpSplit = {
  top : 20,
  left : 20,
  properties : {
    title : 'Nlp Sentence Split',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "NlpSplit",
        "attribute": "",
        "resultAttribute": "splitText",
        "splitOption": "oneToMany",
    },
    image : '../thirdparty/images/sentencesplit.png',
    color : '#e6e600',
  }
}

let sampler = {
  top : 20,
  left : 20,
  properties : {
    title : 'Sampling',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "Sampler",
        "sampleSize": 10,
        "sampleType": "firstk"
    },
    image : '../thirdparty/images/sampling.png',
    color : '#ffdb4d',
  }
}

let projection = {
  top : 20,
  left : 20,
  properties : {
    title : 'Projection',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "Projection",
        "attributes": []
    },
    image : '../thirdparty/images/projection.png',
    color : '#ffdb4d',
      }
  }

let asterixSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Asterix',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "AsterixSource",
        "host": "texera.ics.uci.edu",
        "port": 19002,
        "dataverse": "twitter",
        "dataset": "ds_tweet",
        "queryField": "text",
        "query": "drug",
        "limit": 100000,
    },
    image : '../thirdparty/images/asterixSource.png',
    color : '#cc99ff',
  }
}

let fileSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: File',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "FileSource",
        "filePath": "",
        "resultAttribute": "",
    },
    image : '../thirdparty/images/fileSource.png',
    color : '#cc99ff',
  }
}

let scanSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Scan',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "ScanSource",
        "tableName": ""
    },
    image : '../thirdparty/images/scan.png',
    color : '#cc99ff',
  }
}

 let twitterfeedSource = {
     top : 20,
     left : 20,
     properties : {
         title : 'Source: TwitterFeed',
         inputs : {
             input_1 : {
                 label : "",
             }
         },
         outputs : {
             output_1 : {
                 label : "",
             }
         },
         attributes : {
             "operatorType": "TwitterFeed",
             "keywordList": [],
             "locationList": "",
             "tweetNum": 10,
             "customerKey": "",
             "customerSecret": "",
             "token": "",
             "tokenSecret": "",
             "languageList": ["en"]
         },
         image: '../thirdparty/images/twitterFeed.png',
         color: '#cc99ff'
     }
 }

let keywordSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Keyword',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "KeywordSource",
        "tableName": "",
        "attributes": [],
        "query": "keyword",
        "luceneAnalyzer": "standard",
        "matchingType": "phrase",
        "spanListName": " "
    },
    image : '../thirdparty/images/keywordSource.png',
    color : '#cc99ff',
  }
}


let dictionarySource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Dictionary',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "DictionarySource",
        "tableName": "",
        "attributes": [],
        "dictionaryEntries": [],
        "luceneAnalyzer": "standard",
        "matchingType": "phrase",
        "spanListName": " "
    },
    image : '../thirdparty/images/dictionary.png',
    color : '#cc99ff',

  }
}

let regexSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Regex',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "RegexSource",
        "tableName": "",
        "attributes": [],
        "regex": "regex",
        "regexIgnoreCase": false,
        "regexUseIndex": true,
        "spanListName": " "
    },
    image : '../thirdparty/images/regex.png',
    color : '#cc99ff',
  }
}

let fuzzyTokenSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: FuzzyToken',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "FuzzyTokenSource",
        "tableName": "",
        "attributes": [],
        "query": "token1 token2 token3",
        "luceneAnalyzer": "standard",
        "thresholdRatio": 0.8,
        "spanListName": " ",
    },
    image : '../thirdparty/images/fuzzySource.png',
    color : '#cc99ff',
  }
}

let wordCountSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Word Count',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "WordCountIndexSource",
        "tableName": "",
        "attribute": "",
    },
    image : '../thirdparty/images/countSource.svg',
    color : '#cc99ff',
  }
}

let wordCount = {
  top : 20,
  left : 20,
  properties : {
    title : 'Word Count',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "WordCount",
        "attribute": "",
	      "luceneAnalyzer": "standard",
    },
    image : '../thirdparty/images/counting.png',
    color : '#ffdb4d',
  }
}

let twitterConverter = {
  top : 20,
  left : 20,
  properties : {
    title : 'Convert Twitter',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "TwitterConverter"
    },
    color : '#ffdb4d',
  }
}

let comparison = {
  top : 20,
  left : 20,
  properties : {
    title : 'Comparison',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "Comparison",
        "attribute": "",
        "comparisonType": "=",
	      "compareTo": "",
    },
    image : '../thirdparty/images/compare.png',
    color : '#ffdb4d',
  }
}

let characterDistanceJoin = {
  top : 20,
  left : 20,
  properties : {
    title : 'Join: Character Distance',
    inputs : {
      input_1 : {
        label : '',
      },
      input_2 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "JoinDistance",
        "innerAttribute": "attr1",
        "outerAttribute": "attr1",
        "spanDistance": 100
    },
    image : '../thirdparty/images/characterjoin.png',
    color : '#ffa366',
  }
}

let similarityJoin = {
  top : 20,
  left : 20,
  properties : {
    title : 'Join: Similarity',
    inputs : {
      input_1 : {
        label : '',
      },
      input_2 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "SimilarityJoin",
        "innerAttribute": "attr1",
        "outerAttribute": "attr1",
        "similarityThreshold": 0.8
    },
    image : '../thirdparty/images/similarityjoin.png',
    color : '#ffa366',
  }
}

let result = {
  top : 20,
  left : 20,
  properties : {
    title : 'View Results',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "ViewResults",
        "limit": 10,
        "offset": 0,
    },
    image : '../thirdparty/images/view-result.png',
    color : '#d2a679',
  }
}

let excelSink = {
  top : 20,
  left : 20,
  properties : {
    title : 'Write Excel',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "ExcelSink",
        "limit": 10,
        "offset": 0,
    }
  }
}

let mysqlSink = {
  top : 20,
  left : 20,
  properties : {
    title : 'Write Mysql',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "MysqlSink",
        "host": "localhost",
        "port": 3306,
        "database": "testDB",
        "table": "testTable",
        "username": "test",
        "password": "test"
    },
    image : '../thirdparty/images/sql.jpg',
    color : '#ff8080',
  }
}

export const DEFAULT_MATCHERS: Data[] = [
    {id: 0, jsonData: regexMatcher},
    {id: 1, jsonData: keywordMatcher},
    {id: 2, jsonData: dictionaryMatcher},
    {id: 3, jsonData: fuzzyMatcher},
    {id: 4, jsonData: nlpEntity},
    {id: 5, jsonData: nlpSentiment},
    {id: 6, jsonData: regexSplit},
    {id: 7, jsonData: sampler},
    {id: 8, jsonData: projection},
    {id: 9, jsonData: scanSource},
    {id: 10, jsonData: keywordSource},
    {id: 11, jsonData: dictionarySource},
    {id: 12, jsonData: regexSource},
    {id: 13, jsonData: fuzzyTokenSource},
    {id: 14, jsonData: characterDistanceJoin},
    {id: 15, jsonData: similarityJoin},
    {id: 16, jsonData: wordCountSource},
    {id: 17, jsonData: wordCount},
    {id: 19, jsonData: result},
    {id: 20, jsonData: excelSink},
    {id: 21, jsonData: comparison},
    {id: 22, jsonData: nlpSplit},
    {id: 23, jsonData: emojiSentiment},
    {id: 24, jsonData: fileSource},
    {id: 25, jsonData: mysqlSink},
    {id: 26, jsonData: nltkSentiment},
    {id: 27, jsonData: asterixSource},
    {id: 28, jsonData: twitterConverter},
    {id: 29, jsonData: twitterfeedSource}
];
