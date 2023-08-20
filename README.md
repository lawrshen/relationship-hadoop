# 项目设计

通过一个综合数据分析案例：“西游释厄传——西游记中的人物关系挖掘”，来学习和掌握 MapReduce 程序设计。使用 MapReduce 完成一个综合性的数据挖掘任务，包括全流程的数据预处理、数据分析、数据后处理等。

# 任务描述

## 任务1：数据预处理
任务描述：从原始的西游记小说的文本中，抽取出与人物互动相关的数据。需要屏蔽与人物关系无关的文本内容，为后面的基于人物共现的分析做准备。

- 数据输入：西游记系列小说文集（未分词）；西游记系列小说中的人名列表。
- 数据输出：分词后保留人名。

中文分词指的是将一个汉字序列切分成一个一个单独的词。分词就是将连续的字序列按照一定的规范重新组合成词序列的过程；在英文中，单词之间是以空格作为自然分界符，汉语中词没有一个形式上的分界符。正因为缺乏形式上的分界符，导致我们对词的认定会出现很大的偏差。在考虑IK，Jieba和Ansj_seg[^1]等一系列第三方包后，选择Ansj_seg作为实验工具，参考了[示例](https://blog.csdn.net/bitcarmanlee/article/details/53607776)的使用方式，用以下代码实现分s词，并遍历判断：

[^1]: https://www.infoq.cn/article/nlp-word-segmentation

```Java
Result result = DicAnalysis.parse(value.toString());
for (Term t : result.getTerms()) {
    if(分词在人名列表中){
        加入输出
    }
}
```

对于 MapReduce 程序而言，使用 Job 的 Configuration 将人名列表文件分发到每台机器上读入内存，由 map 部分的 setup 预处理，再使用上述分词方法得到分词后的人名列表，reduce 部分没有实际工作。

执行命令

```SHELL
hadoop jar proj.jar preprocess.preprocess /data/relation/xiyouji /user/proj-out1/ /data/relation/xiyouji_name_list.txt
```

## 任务2：人物同现统计

任务描述：完成基于单词同现算法的人物同现统计。在人物同现分析中，如果两个人在原文的同一段落中出现，则认为两个人发生了一次同现关系。我们需要对人物之间的同现关系次数进行统计，同现关系次数越多，说明两人之间的关系越密切。

- 数据输入：任务1 的输出
- 数据输出：人物之间的同现次数

两个名字在同一行出现，则记 1 次。但同一个人的人名出现两次只看作 1 次，主要人物的名称也要进行统一，视为一次。其中主要人物及其别名已经在人名列表文件的开头给出，采取硬编码方式在 setup 部分设置映射，最后通过集合提供的唯一性特征完成要求。

### Mapper 
同一行中的人名由空格`" "`隔开，分割为单个人名

因为重复人名只记一次，所以遍历时需要删除重复人名，但是 String[]不能直接删除，所以采用 Set<String> filterName 进行过滤，

```JAVA
for (int i = 0; i < names.length; i++) {
    if (主要角色多名){
        filterName.add(统一名字);
    } else {
        filterName.add(names[i]);
    }
}
```

再转换回 String[] ，对两个不同的人之间的同现关系保存为格式：name1,name2

将这个同现关系（name1,name2）作为 key，1作为 value 传递给 reducer 求和即可得知两人同现次数。 

### Reducer
for(Text value:values) 
 计数; 

然后以<key,次数>的格式输出 

### 实验结果

执行命令

```SHELL
hadoop jar proj.jar coocurrence.coocurrence /user/proj-out1/part-r-00000 /user/proj-out2/ 
```

## 任务3：人物关系图构建与特征归一化
任务描述：根据共现关系，生成人物之间的关系图。任务关系图使用邻接表形式表示，方便后续的 PageRank 计算。人物关系图中，人物是顶点，人物间的互动关系是边，人物互动关系靠人物间的共现关系决定。如果两个人之间具有共现关系，则两个人之间就具有一条边。两人之间的共现次数体现出两人关系的密切程度，反映到共现关系图上就是边的权重。权重越高，两人关系越密切。

- 数据输入：任务2 的输出。
- 数据输出：人物关系图。

这一实验是为了使后面的分析方便，需要对共现次数进行归一化处
理：将共现次数转换为共现概率。

### Mapper 
输入格式：人名1，人名2 \t 次数 

Mapper 中将人名分开，人名1作为key，而将人名2和次数合并输出为 value 
输出格式：人名1 \t 人名2，次数>

### Reducer 
Reducer 中统计其他人名的同现次数之和sum；freq[i]/sum 就是name[i]关于key 的同现概率 

### 实验结果

执行命令

```SHELL
hadoop jar proj.jar normalization.normalization /user/proj-out2/part-r-00000 /user/proj-out3/ 
```

## 任务4：基于人物关系图的PageRank计算

任务描述：计算PageRank，定量分析小说的主角。

- 数据输入：任务3的输出
- 数据输出：各人物的PageRank值

### 初始化数据

此阶段任务在Mapper中完成即可，不需要Reducer。

**输入**

人名1 \t [人名2，权重2 | 人名3，权重3 | ...]

**输出**

人名1 \t 初始PageRank值 \t [人名2，权重2 | 人名3，权重3 | ...]

### 迭代计算PageRank值

#### Mapper

**输入**

人名1 \t PageRank值 \t [人名2，权重2 | 人名3，权重3 | ...]

**输出**

人名1  \t |[人名2，权重2 | 人名3，权重3 | ...]

人名2 \t PageRank值 * 权重2

人名3 \t PageRank值 * 权重3

...

```java
void map(key, value, context) {
    name, pr, links = value.split("\t");
    for link in links {
        n, weight = link.split(",");
        emit(n, double(pr * weight));
    }
    emit(name, "|" + links);
}
```

#### Reducer

**输入**

人名1 \t Iterable<[人名2，权重2 | 人名3，权重3 | ...]，PageRank值2 * 权重2，PageRank值3 * 权重3， ...>

**输出**

人名1 \t 新PageRank值 \t [人名2，权重2 | 人名3，权重3 | ...]

```java
void reduce(key, values, context) {
    double pr = 0;
    String links = "";
    for value in values {
        if (value.startsWith("|")) {
            links = value.substring(1);
        } else {
            pr += value;
        }
    }
    pr = 1.00 - 0.85 + 0.85 * pr;
    emit(key, pr + "\t" + links);
}
```

### 排序

按照PageRank值，通过一次MapReduce进行排序。

### 实验结果

执行命令

```SHELL
hadoop jar proj.jar pagerank.PageRank /user/proj-out3/part-r-00000 /user/proj-out4/ /user/tmp/
```

## 任务5：人物关系图上的标签传播

任务描述：实现标签传播算法。标签传播是一种半监督图分析算法，通过在图上顶点打标签，进行图顶点的聚类分析，从而在一张社交网络图中完成社区发现。

- 数据输入：任务3的输出
- 数据输出：人物标签信息

### 初始化数据

构造原图的转置图

### Mapper

**输入**

人名1 \t [人名2，权重2 | 人名3，权重3 | ...]

**输出**

人名2 \t [人名1，权重2]

人名3 \t [人名1，权重3]

...

### Reducer

根据收到的键值对输出转置图，再加上初始标签即可。

### 迭代过程

在Reducer中，需要预先知道上一轮迭代的标签，所以Reducer应先收到图结构，再接收其他键值对。因此需要对Mapper输出的键做一些处理。

#### Mapper

**输入**

人名1 \t 标签 \t [人名2，权重2 | 人名3，权重3 | ...]

**输出**

人名1,a  \t  标签  \t  [人名2，权重2 | 人名3，权重3 | ...]

人名2,b  \t  [标签，权重2]

人名3,b  \t  [标签，权重3]

...

```java
void map(key, value, context) {
    name, label, links = value.split("\t");
    for link in links {
        n, weight = link.split(",");
        emit(n + ",b", label + "," + weight);
    }
    emit(name + ",a", label + "\t" + links);
}
```

#### Partitioner

根据Mapper输出的键中的人名进行划分。

#### Reducer

新标签修改为对应权重最大的标签。如果有多个这样的标签（最大权重不唯一），则优先选择上一轮的标签。

**输入**

人名1,a  \t  Iterable<[标签  \t  [人名2，权重2 | 人名3，权重3 | ...]>

人名1,b  \t  Iterable<[标签2，权重2]，[标签3，权重3]， ...>

**输出**

人名1 \t 新标签 \t [人名2，权重2 | 人名3，权重3 | ...]

```java
class LPAIterReducer {
    name = "";
    oldLabel = "";
    links = "";
    
    void reduce(key, values, context) {
        n, type = key.split(",");
        if (type.equals("a")) {
            name = n
            oldLabel, links = values.next().split("\t");
        } else {
            HashMap<String, Double> mp = new HashMap<String, Double>();
            for value in values {
                String[] u = value.split(",");
                mp[u[0]] += u[1];
            }
            double maxVal = 0.0;
            String newLabel = "";
            for (String k : mp.keySet()) {
                double val = mp.get(k);
                if (val > maxVal || (val == maxVal && !newLabel.equals(oldLabel))) {
                    maxVal = val;
                    newLabel = k;
                }
            }
            emit(name, newLabel + "\t" + links);
        }
    }
}
```

### 同一标签归类

通过一次MapReduce归类即可。

### 实验结果

执行命令

```SHELL
hadoop jar proj.jar lpa.LPA /user/proj-out3/part-r-00000 /user/proj-out5/ /user/tmp/
```