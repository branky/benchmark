A = load '/user/bishao/wordcount-3.txt';
B = foreach A generate flatten(TOKENIZE((chararray)$0)) as word;
C = filter B by word matches '\\w+';
D = group C by word;
E = foreach D generate COUNT(C), group;
store E into '/user/bishao/wcoutpig';