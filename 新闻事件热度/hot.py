#!/usr/bin/env python
# coding:utf-8

from optparse import OptionParser
import sys
import time
import hashlib
import re


def encryptionMd5(url):
    '''The URL using MD5 encryption
    :param url: the event's url
    :return: using MD5 encryption
    '''
    m = hashlib.md5()
    m.update(url)
    md5_url = m.hexdigest()
    return md5_url


def getClusterDic(cluster_data, cluster_url, cluster_cluster):
    '''Get the list of the comment number
        Args:
            cluster_data : the path of cluster file
            cluster_url : the index of uuid in the file of cluster_data
            cluster_cluster : the index of cluster events in the file of cluster_data
        Returns:
            The dictionary of cluster
    '''
    clusterDic = {}
    stdin = sys.stdin if cluster_data is None else open(cluster_data, "rb")
    for obj in stdin:
        obj = obj.strip().split("\001")
        cluster = obj[cluster_cluster].strip().split()
        clusterDic[obj[cluster_url]] = cluster
    return clusterDic


def getClusterScore(cluster_data, cluster_url, cluster_cluster):
    '''Get the Dictionary of the cluster
        Args:
            cluster_data : the path of cluster file
            cluster_url : the index of uuid in the file of cluster_data
            cluster_cluster : the index of cluster events in the file of cluster_data
        Returns:
            The dictionary of cluster
    '''
    clusterscoreDic = {}
    stdin = sys.stdin if cluster_data is None else open(cluster_data, "rb")
    for obj in stdin:
        obj = obj.strip().split("\001")
        cluster = obj[cluster_cluster].strip().split()
        if len(cluster) == 1:
            clusterscoreDic[obj[cluster_url]] = 0
        elif len(cluster) == 2:
            clusterscoreDic[obj[cluster_url]] = 10
        elif len(cluster) == 3:
            clusterscoreDic[obj[cluster_url]] = 20
        elif len(cluster) > 3:
            clusterscoreDic[obj[cluster_url]] = 30
    return clusterscoreDic


def getHotElements(data, urlIndex, commentIndex, readIndex, datasourceIndex, timeIndex):
    '''Get the elements of the hot,including comments /reads
            Args:
                data : the path of the data
                urlIndex : the index of url
                commentIndex : the index of comments
                readIndex : the index od read
            Returns:
                The dictionary of hotelement
    '''
    stdin = sys.stdin if data is None else open(data, "rb")
    hot_element = {}
    for obj in stdin:
        obj = obj.strip().split("\001")
        if obj[commentIndex] != 0 and len(obj[commentIndex]) != 0 and obj[readIndex] != 0 and len(obj[readIndex]) != 0:
            hot_element[obj[urlIndex]] = {"comments": obj[commentIndex], "reads": obj[readIndex],
                                          "time": obj[timeIndex], "source": obj[datasourceIndex]}
        elif obj[commentIndex] == 0 and obj[readIndex] == 0:
            hot_element[obj[urlIndex]] = {"comments": 1, "reads": 1, "time": obj[timeIndex],
                                          "source": obj[datasourceIndex]}
        elif obj[commentIndex] == 0 or len(obj[commentIndex]) == 0:
            hot_element[obj[urlIndex]] = {"comments": 1, "reads": obj[readIndex], "time": obj[timeIndex],
                                          "source": obj[datasourceIndex]}
        elif obj[readIndex] == 0 or len(obj[readIndex]) == 0:
            hot_element[obj[urlIndex]] = {"comments": obj[commentIndex], "reads": 1, "time": obj[timeIndex],
                                          "source": obj[datasourceIndex]}
    return hot_element


# 热度计算模型部分
def hotscore(new_time, comments, reads, clusterscores, time_values):
    if len(new_time) > 0:
        numbers = (comments + 0.2 * reads) / 2 + float(clusterscores)
        hot = numbers / time_values
    else:
        hot = 0.0
    return hot


def main(data, cluster_data, indexes, out):
    '''Get the HotScore
            Args:
                data : the path of  files like "2017021717.news_zhengwen"
                cluster_data : the path of cluster file
                indexes : the index of urlIndex, commentIndex, timeIndex, readIndex, cluster_url, cluster_cluster
                        like (14|22|10|15|1|9)
                out : the path of out
            Returns:
                The hot of the news
    '''
    # 文件名时间装换为时间戳作为当前时间
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    timeSet = data.split('.')[-2][-10:-2:1]
    now_time = time.mktime(time.strptime(timeSet, "%Y%m%d"))

    stdin = sys.stdin if cluster_data is None else open(cluster_data, "rb")
    stdout = sys.stdout if out is None else open(out, "wb")
    urlIndex, commentIndex, timeIndex, readIndex, datasourceIndex, cluster_url, cluster_cluster = map(
        lambda i: int(i) - 1,
        indexes.strip().split('|'))
    cluster_dic = getClusterDic(cluster_data, cluster_url, cluster_cluster)
    clusterscore = getClusterScore(cluster_data, cluster_url, cluster_cluster)
    hotelement_dic = getHotElements(data, urlIndex, commentIndex, readIndex, datasourceIndex, timeIndex)
    hot_dic = {}
    # num=0
    for obj in stdin:
        # num+=1
        # print num
        obj = obj.strip().split("\001")
        url = obj[cluster_url]
        # md5_url = encryptionMd5(url)
        # 获取新闻发布时间并对时间格式做处理
        # cluster=obj[cluster_cluster]
        new_time = hotelement_dic[url]["time"]
        datasource = hotelement_dic[url]['source']
        timestamp = time.mktime(time.strptime(new_time, "%Y-%m-%d %H:%M:%S"))
        # 间隔天数
        time_value = (now_time - float(timestamp)) / (24 * 60 * 60)
        hot_list = []
        for line in cluster_dic[url]:
            comments = int(hotelement_dic[line]["comments"])
            reads = int(hotelement_dic[line]["reads"])
            clusterscores = clusterscore[obj[cluster_url]]
            hots = hotscore(new_time, comments, reads, clusterscores, time_value)
            hot_list.append(hots)
        hots = max(hot_list)
        hot_dic[url] = {}
        hot_dic[url]["datasource"] = datasource
        hot_dic[url]["hot"] = hots
        hot_dic[url]["new_time"]=new_time

    listwangyixinwen = []
    listxinlangwang = []
    listsouhuwang = []
    listfenghuangwang = []
    listxinhuawang = []
    listjinritoutiao = []
    for i, j in hot_dic.iteritems():
        if j["datasource"] == '网易新闻':
            listwangyixinwen.append(j['hot'])
            hot = (j['hot'] - min(listwangyixinwen)) / (max(listwangyixinwen) + 5) * 100
            md5_obj = encryptionMd5(i)
            stdout.write(i + "\001" + str(md5_obj) + "\001" + str(hot) + "\n")
        if j["datasource"] == '新浪网':
            listxinlangwang.append(j['hot'])
            hot = (j['hot'] - min(listxinlangwang)) / (max(listxinlangwang) + 5) * 100
            md5_obj = encryptionMd5(i)
            stdout.write(i + "\001" + str(md5_obj) + "\001" + str(hot) + "\n")
        if j["datasource"] == '搜狐网':
            listsouhuwang.append(j['hot'])
            hot = (j['hot'] - min(listsouhuwang)) / (max(listsouhuwang) + 5) * 100
            md5_obj = encryptionMd5(i)
            stdout.write(i + "\001" + str(md5_obj) + "\001" + str(hot) + "\n")
        if j["datasource"] == '凤凰网':
            listfenghuangwang.append(j['hot'])
            hot = (j['hot'] - min(listfenghuangwang)) / (max(listfenghuangwang) + 5) * 100
            md5_obj = encryptionMd5(i)
            stdout.write(i + "\001" + str(md5_obj) + "\001" + str(hot) + "\n")
        if j["datasource"] == '新华网':
            listxinhuawang.append(j['hot'])
            hot = (j['hot'] - min(listxinhuawang)) / (max(listxinhuawang) + 5) * 100
            md5_obj = encryptionMd5(i)
            stdout.write(i + "\001" + str(md5_obj) + "\001" + str(hot) + "\n")
        if j["datasource"] == '今日头条':
            listjinritoutiao.append(j['hot'])
            hot = (j['hot'] - min(listjinritoutiao)) / (max(listjinritoutiao) + 5) * 100
            md5_obj = encryptionMd5(i)
            stdout.write(i + "\001" + str(md5_obj) + "\001" + str(hot) + "\n")
            # print  hot
            # print list
            # print i,j['hot']
            # print i,j["datasource"],j["hot"]
            # print j['hot']
    print "ok"
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))

if __name__ == "__main__":
    parser = OptionParser(usage="%prog -d data -c cluster_data -i indexes -o out")

    parser.add_option(
        "-d", "--data",
        help=u"The file name of the data like /data2/event_analyes/2017021717.news_zhengwen"
    )

    parser.add_option(
        "-c", "--cluster_data",
        help=u"The file name of the cluster of same incident /data2/event_analyes/data/cluster/2017021717.cluster"
    )

    parser.add_option(
        "-i", "--indexes",
        help=u"Array of index in the order of urlIndex|commentIndex|timeIndex|readIndex|cluster_url|cluster_cluster like (14|22|10|15|1|9)"
    )

    parser.add_option(
        "-o", "--out",
        help=u"The file name of the out"
    )

    # if not sys.argv[1:]:
    #     parser.print_help()
    #     exit(1)

    # (opts, args) = parser.parse_args()
    # main(data=opts.data, cluster_data=opts.cluster_data, indexes=opts.indexes, out=opts.out)
    main(data=r'E:\data3\event_analyes\data\2017091200.news_zhengwen',
         cluster_data=r'E:\data3\event_analyes\data\cluster\2017091200.cluster',
         indexes='6|12|4|13|7|1|9', out=r'E:\data3\event_analyes\data\hot0912hot')
    # python /data2/event_analyes/py/hot_geyi1.py -d /data2/event_analyes/data/2017021717.news_zhengwen -c /data2/event_analyes/data/cluster/2017021717.cluster -i "14|22|10|15|1|9"  -o /data2/event_analyes/data/hot_value/2017021717.hot
