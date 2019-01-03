#-*- coding: utf-8 -*-
import pymongo
import pandas as pd
import numpy as np
from pyecharts import Bar
from matplotlib.font_manager import FontProperties
import os
from pylab import mpl
from pyecharts import Pie, Line, Funnel, Scatter
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
from PIL import Image
import jieba
pd.options.display.max_columns = 999


class analisis():
    '''

    '''
    _loc = ["天河", "白云", "番禺", "从化", "越秀", "海珠", "花都", "增城",
                  "荔湾", "黄埔", "南沙"]
    font = FontProperties(fname=r"c:\windows\fonts\simsun.ttc", size=14)

    def __init__(self, client, db, collection):
        '''
        : 连接数据库
        :param client:
            mongodb服务器
        :param db:
            数据所在数据库
        :param
            collection: 数据所在集合
        '''
        try:
            self.client = client
            self.db = db
            self.collection = collection
            # self.collection2 = collection2
            print(f'已连接数据库{self.collection}')
        except Exception as e:
            print(f'无法连接数据库{self.collection}')

    def cleanDB(self):
        '''

        :param collection:
            传入清理的集合, 项目中为collection1(house)/2(updated_house)
        :return:
            返回清理数量
        '''
        try:
            result = self.collection.delete_many({"$or":[{"price":None, "area":None}]})
            print(f"成功清理{self.collection}价格/面积为null的数据{result.deleted_count}条")
        except Exception as e:
            print("清理db失败", e)

    def cleanData(self, file_path):
        '''
        清洗数据, 删除不需要的列, 将数据替换成想要的格式, 导为csv文件
        :param file_path:
            导出文件路径, 名字.
        :return:
            返回一个csv文件, 将此csv文件导入到mongodb中
        '''
        try:
            house = pd.read_csv(r'E:\Cache\CacheOfJup\pandas & excel\ftx_data\ftx.house.csv', index_col=None)
            # 删掉不必要的列
            # 可以在这里自定义删除的列
            house.drop(columns=['_id', 'quarters.0', 'quarters.2', 'quarters.3', 'quarters.4',
                                'renovation', 'title.0','apartment',
                                'floor', 'lease_mode'], inplace=True)
            # 转换数据
            house['area'] = house['area'].str.replace("平米", "")
            house['loc'] = house['quarters.1']
            house.drop(columns="quarters.1", inplace=True)
            # 写出到excel中
            house.to_csv(file_path+'.csv')
            print(f'成功清理数据, 将目标文件写入到{file_path}.csv中!')
        except Exception as e:
            print("清理数据失败: ", e)

    '''
    更改格式: 
    db.getCollection("house").find({"price":{"$type":2}}).forEach(function(x){
	x.price = NumberInt(x.price);
	db.getCollection("house").save(x);
    })
    '''
    def getWordCloudData(self):
        '''
        获取词云文本数据
        :return:
            返回文件名称
        '''
        try:
            # 删除title为空的数据
            self.collection.delete_many({"title": None})
            word_results = self.collection.find({}, {'_id': False, 'title': True})
            content = []
            for word_result in word_results:
                # print(word_result['title'])
                # content += str(word_result["title"])
                content.append(word_result["title"])
            # print(content)
            # print(content)
            with open('content.csv', 'w+', encoding='GBK') as f:
                for line in content:
                    f.write(str(line))
                print("成功获取词云文本数据")
                return f.name

        except Exception as e:
            print("无法获取词云文本数据: ", e)


    def getAreaWeight(self):
        '''
        计算每个地区房源数量
        :return:
            返回_loc和计算结果
        '''
        try:
            area_count = []
            for i in self._loc:
                area_count.append(self.collection.count_documents({"quarters.1": i}))
                # v1 : area_count.append(self.collection.find({"quarters.1": i}).count())
            return self._loc, area_count
        except Exception as e:
            print("无法获取房源数量比重: ", e)

    def getPrice_AreaAvg(self):
        '''
        计算各地区每平方米的价格(月)
        :return:
            返回一个list
        '''
        avg_ = []
        for i in self._loc:
            # 每个地区总价
            price = self.collection.aggregate([
                {"$match": {"loc": i}},
                {"$group": {"_id": "", "sumPrice": {"$sum": "$price"}}}
            ])
            # 每个地区总面积
            area = self.collection.aggregate([
                {"$match": {"loc": i}},
                {"$group": {"_id": "", "sumArea": {"$sum": "$area"}}}
            ])
            # 每个地区平均值(元/平米)
            avg = list(price)[0]['sumPrice'] / list(area)[0]['sumArea']
            avg_.append(avg)
            # print(list(price)[0]['sumPrice'])
            # print(list(area)[0]['sumArea'])

        # print(avg_)
        return avg_
    def getPriceAvg(self):
        '''
        获取各个区价格平均值
        :return:
            返回一个list
        '''
        price_avg = []
        for i in self._loc:
            price = self.collection.aggregate([
                {"$match": {"loc": i}},
                {"$group": {"_id": "", "price_avg": {"$avg": "$price"}}}
            ])
            price_avg.append(list(price)[0]["price_avg"])
        return price_avg

    def getPriceInterval_avg(self):
        '''
        获得*价格区间内*房源总数, 平均价格, 平均面积
        :return:
            返回两个list > 平均价格, 平均面积
        '''
        _avg_area = []
        _avg_price = []
        _total = []
        # _loc_x = [] # 存储区间, 测试用

        for i in range(0, 57000, 3000):
            _total.append(self.collection.count_documents({"$and": [{"price": {"$lte": i + 3000, "$gt": i}}]}))
            _area = self.collection.aggregate([
                {"$match": {"price": {"$lte": i + 3000, "$gt": i}}},
                {"$group": {"_id": "", "avg_area": {"$avg": "$area"}}}
            ])
            _price = self.collection.aggregate([
                {"$match": {"price": {"$lte": i + 3000, "$gt": i}}},
                {"$group": {"_id": "", "avg_price": {"$avg": "$price"}}}
            ])
            _avg_area.append(list(_area)[0]['avg_area'])
            _avg_price.append(list(_price)[0]['avg_price'])
            # _loc_x.append([i, i + 3000])
        return _avg_area, _avg_price

class draw(analisis):
    def line_priceAvg(self):
        y = self.getPriceAvg()
        line = Line("房价平均值折线图")
        line.add("price", self._loc, y,
                 mark_line=["max", "average", "min"],
                 yaxis_name="单元:元",
                 yaxis_name_gap=43)
        line.render(path="price-line.html")

    def scatter_plt_AvgArea_Price(self):
        _avg_area, _avg_price = self.getPriceInterval_avg()

        plt.scatter(x=_avg_area, y=_avg_price)
        # plt.scatter(x=ar_, y=pr_) # 价格-面积散点图
        plt.title('广州市', fontproperties=self.font)
        plt.xlabel('平均面积范围:[76.5, 800] 单位:平米', fontproperties=self.font)
        plt.ylabel('平均房价范围:[0, 57000, step=3000] 单位:元', fontproperties=self.font)
        plt.show()

    def scatter_echar_AvgArea_price(self):
        x, y = self.getPriceInterval_avg()

        scatter = Scatter("广州市租房平均面积与平均价格关系图")
        scatter.add("平均面积-平均价格", x, y,
                    yaxis_name="平均价格(元)",
                    xaxis_name="平均面积(平米)",
                    yaxis_name_gap=50,
                    )
        scatter.render('scatter_avgAreaPrice.html')

    def line_avg_price_area(self):
        attr = self._loc
        values = self.getPrice_AreaAvg()
        from pyecharts import Line
        line = Line("广州市租房租房单价:平方米/月")
        line.add('平方米/月', attr, values, mark_line=["average"],
                 is_smooth=False, is_step=True,
                 yaxis_name="每平方米价格(元)",
                 yaxis_name_gap=35,
                 mark_point=[{"coord": ["天河", 71.5], "name": "天河每平方米房价是"},
                             {"coord": ["白云", 40.5], "name": "白云每平方米房价是"},
                             {"coord": ["番禺", 39.7], "name": "番禺每平方米房价是"},
                             {"coord": ["从化", 14.0], "name": "从化每平方米房价是"},
                             {"coord": ["越秀", 60.7], "name": "越秀每平方米房价是"},
                             {"coord": ["海珠", 58.1], "name": "海珠每平方米房价是"},
                             {"coord": ["花都", 22.1], "name": "花都每平方米房价是"},
                             {"coord": ["增城", 17.7], "name": "增城每平方米房价是"},
                             {"coord": ["荔湾", 37.5], "name": "荔湾每平方米房价是"},
                             {"coord": ["黄埔", 37.8], "name": "黄埔每平方米房价是"},
                             {"coord": ["南沙", 22.6], "name": "南沙每平方米房价是"}],
                 )
        line.render(path='lineAvg.html')

    def wordcloud_title(self):
        fname = self.getWordCloudData()
        wordcloud_content = open(fname, 'r', encoding="GBK").read()
        wc_font = 'c:/windows/Fonts/simhei.ttf'

        # 分词
        content_split = ''
        content_split += ''.join(jieba.cut(wordcloud_content, cut_all=False))
        pircture = np.array(Image.open('../data/princess.png'))
        draw = WordCloud(
            font_path=wc_font,
            background_color='white',
            stopwords=STOPWORDS,
            collocations=False,
            width=1000,
            height=800,
            mask=pircture
        ).generate(content_split)

        plt.imshow(draw, interpolation='bilinear')
        # draw.to_file('')
        plt.axis('off')
        draw.to_file('wordcloud_title.png')
        plt.show()

    def bar_AreaWeight(self):
        attr, data = self.getAreaWeight()
        x = np.arange(11)

        bar = Bar('房源统计')
        bar.add("广州", attr, data,
                yaxis_name='房源数量',
                yaxis_name_gap=42,
                mark_line=["average"],
                mark_point=["max", "min"])
        bar.render(path='bar_AreaWeight.html')


class mgb():
    '''
    horse's code
    '''
    path = r'../data/'
    all_house_path = r'../data/All_house.csv'

    # a = pd.read_csv(all_house_path)
    # print(a.head(3))
    # 获取文件夹中所有子文件
    def GetFileList(self, dir, fileList):
        newDir = dir
        if os.path.isfile(dir):
            fileList.append(dir)
        elif os.path.isdir(dir):
            for s in os.listdir(dir):
                newDir = os.path.join(dir, s)
                self.GetFileList(newDir, fileList)
        return fileList

    # pie图
    def renovation_pie(self):
        data = pd.read_csv(self.all_house_path, encoding='utf-8')

        mpl.rcParams['font.sans-serif'] = ['FangSong']  # 指定默认字体  # 调节matplotlib支持中文字体
        mpl.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'-'显示不了的问题
        count = data['renovation'].value_counts()
        # attr = count.index
        attr = list(count.index)
        values = list(count.values)
        # print(attr)
        # print(values)

        pie = Pie('广州租房装修比重', title_pos='65%')
        pie.add(
            "",
            attr,
            values,
            radius=[40, 75],
            label_text_color=None,
            is_label_show=True,
            legend_orient="vertical",
            legend_pos="left",
            rosetype='radius'
        )
        pie.render(path='pie_decorated.html')

    def wordcloud_apartment(self):
        wordcloud_data = pd.read_csv(self.all_house_path, encoding='utf-8')
        wordcloud_data = wordcloud_data.groupby(by='apartment').count()
        wordcloud_data = wordcloud_data['price'].sort_values(ascending=False)
        # wordcloud_data = dict(wordcloud_data)
        # print(wordcloud_data)

        font = '‪C:\\Windows\\Fonts\\simhei.ttf'  # 设置中文字体
        wc = WordCloud(font_path=font,  # 如果是中文必须要添加这个，否则会显示成框框,英文可不添加
                       background_color='#F3F3F3',
                       stopwords=None,  # 停止词设置
                       width=1000,
                       height=800,
                       random_state=2,
                       scale=2,
                       ).generate_from_frequencies(wordcloud_data)
        plt.imshow(wc, interpolation='bilinear')
        wc.to_file('wordcloud_apartment.png')  # 保存图片
        plt.axis('off')  # 不显示坐标轴
        plt.show()  # 显示图片

    # 朝向漏斗图
    def funnel_orientataion(self):
        orientation = pd.read_csv(self.all_house_path, encoding='utf-8')
        orientation = orientation.groupby(by='orientation').count()
        orientation = orientation['price'].sort_values(ascending=False)
        attr = orientation.index
        value = orientation.values
        index = []
        values = []

        for i in attr:
            index.append(i)
        for i in value:
            values.append(i)
        # print(len(index), len(value))

        funnel_orien = Funnel("房源朝向漏斗图", width=800, height=700, title_pos="center")
        funnel_orien.add(
            "朝向",
            attr,
            value,
            is_label_show=True,
            label_pos="outside",
            legend_pos="left",
            label_text_color="#000",
            legend_orient="vertical",
            funnel_gap=5,
        )
        funnel_orien.render(path="funnel-orientataion.html")


def main():
    # 测试
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client.ftx
    collection = db.updated_house
    collection2 = db.house
    try:
        co1 = draw(client, db, collection)
        co2 = draw(client, db, collection2)

        co1.cleanDB()
        co2.cleanDB()

        m = mgb()

        co2.getWordCloudData()
        # 获取词云

        co2.wordcloud_title()
        co2.bar_AreaWeight()

        co1.line_priceAvg()
        co1.line_avg_price_area()
        co1.scatter_echar_AvgArea_price()

        m.funnel_orientataion()
        m.wordcloud_apartment()
        m.renovation_pie()

        co1.scatter_plt_AvgArea_Price()

        print("画图完毕")
    except Exception as e:
        print('画图失败:', e)



main()