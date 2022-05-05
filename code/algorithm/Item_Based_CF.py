#-*- coding: utf-8 -*-

import sys
import random
import math
import os
from operator import itemgetter

from collections import defaultdict

random.seed(0)


class ItemBasedCF(object):
    ''' TopN recommendation - Item Based Collaborative Filtering '''

    def __init__(self):
        self.trainset = {}
        self.testset = {}

        # 存储 id 对 name的映射
        self.id2name = {}
        with open("../../data/item_cf_train.csv","r",encoding="utf-8") as f:
            for i in f:
                i = i.strip().split("\t")
                self.id2name[i[1]] = i[-1]
        # 相似产品数       
        self.n_sim_produce = 20
        # 推荐数
        self.n_rec_produce = 5
        # 物品相似矩阵
        self.produce_sim_mat = {}
        self.produce_popular = {}
        self.produce_count = 0



        print('Similar produce number = %d' % self.n_sim_produce, file=sys.stderr)
        print('Recommended produce number = %d' %
              self.n_rec_produce, file=sys.stderr)

    @staticmethod
    def loadfile(filename):
        ''' load a file, return a generator. '''
        fp = open(filename, 'r', encoding="utf-8")
        for i, line in enumerate(fp):
            yield line.strip('\r\n')
            if i % 100000 == 0:
                print ('loading %s(%s)' % (filename, i), file=sys.stderr)
        fp.close()
        print ('load %s succ' % filename, file=sys.stderr)

    def generate_dataset(self, filename, pivot=0.7):
        ''' load rating data and split it to training set and test set '''
        trainset_len = 0
        testset_len = 0
        #{"user1":{"item1":3,"item2":4},"user2"}
        for line in self.loadfile(filename):
            user, produce, rating, _,_ = line.split('\t')
            # split the data by pivot
            if random.random() < pivot:
                self.trainset.setdefault(user, {})
                self.trainset[user][produce] = int(rating)
                trainset_len += 1
            else: 
                self.testset.setdefault(user, {})
                self.testset[user][produce] = int(rating)
                testset_len += 1

        print ('split training set and test set succ', file=sys.stderr)
        print ('train set = %s' % trainset_len, file=sys.stderr)
        print ('test set = %s' % testset_len, file=sys.stderr)

    def calc_produce_sim(self):
        ''' calculate produce similarity matrix '''
        print('counting produces number and popularity...', file=sys.stderr)

        for user, produces in self.trainset.items():
            for produce in produces:
                # count item popularity
                if produce not in self.produce_popular:
                    self.produce_popular[produce] = 0
                self.produce_popular[produce] += 1

        print('count produces number and popularity succ', file=sys.stderr)

        # save the total number of produces
        self.produce_count = len(self.produce_popular)
        print('total produce number = %d' % self.produce_count, file=sys.stderr)

        # count co-rated users between items
        itemsim_mat = self.produce_sim_mat
        print('building co-rated users matrix...', file=sys.stderr)
        # {"item1":{"item2":2,"item3":1},"item2":{"item1":1,"item4":5}}
        for user, produces in self.trainset.items():
            for m1 in produces:
                itemsim_mat.setdefault(m1, defaultdict(int))
                for m2 in produces:
                    if m1 == m2:
                        continue
                    itemsim_mat[m1][m2] += 1

        print('build co-rated users matrix succ', file=sys.stderr)

        # calculate similarity matrix
        print('calculating produce similarity matrix...', file=sys.stderr)
        simfactor_count = 0
        PRINT_STEP = 2000000

        for m1, related_produces in itemsim_mat.items():
            for m2, count in related_produces.items():
                itemsim_mat[m1][m2] = count / math.sqrt(
                    self.produce_popular[m1] * self.produce_popular[m2])
                simfactor_count += 1
                if simfactor_count % PRINT_STEP == 0:
                    print('calculating produce similarity factor(%d)' %
                          simfactor_count, file=sys.stderr)

        print('calculate produce similarity matrix(similarity factor) succ',
              file=sys.stderr)
        print('Total similarity factor number = %d' %
              simfactor_count, file=sys.stderr)

    def recommend(self, user):
        ''' Find K similar produces and recommend N produces. '''
        K = self.n_sim_produce
        N = self.n_rec_produce
        rank = {}
        watched_produces = self.trainset[user]

        for produce, rating in watched_produces.items():
            for related_produce, similarity_factor in sorted(self.produce_sim_mat[produce].items(),
                                                           key=itemgetter(1), reverse=True)[:K]:
                if related_produce in watched_produces:
                    continue
                rank.setdefault(related_produce, 0)
                rank[related_produce] += similarity_factor * rating
        # return the N best produces
        return sorted(rank.items(), key=itemgetter(1), reverse=True)[:N]
    

    def evaluate(self):
        ''' print evaluation result: precision, recall, coverage and popularity '''
        print('Evaluation start...', file=sys.stderr)

        N = self.n_rec_produce
        #  varables for precision and recall
        hit = 0
        rec_count = 0
        test_count = 0
        # varables for coverage
        all_rec_produces = set()
        # varables for popularity
        popular_sum = 0

        for i, user in enumerate(self.trainset):
            if i % 500 == 0:
                print ('recommended for %d users' % i, file=sys.stderr)
            test_produces = self.testset.get(user, {})
            rec_produces = self.recommend(user)
            for produce, _ in rec_produces:
                if produce in test_produces:
                    hit += 1
                all_rec_produces.add(produce)
                popular_sum += math.log(1 + self.produce_popular[produce])
            rec_count += N
            test_count += len(test_produces)

        precision = hit / (1.0 * rec_count)
        recall = hit / (1.0 * test_count)
        coverage = len(all_rec_produces) / (1.0 * self.produce_count)
        popularity = popular_sum / (1.0 * rec_count)

        print ('precision=%.4f\trecall=%.4f\tcoverage=%.4f\tpopularity=%.4f' %
               (precision, recall, coverage, popularity), file=sys.stderr)


if __name__ == '__main__':
    ratingfile = os.path.join('../../data/item_cf_train.csv')
    itemcf = ItemBasedCF()
    itemcf.generate_dataset(ratingfile)
    itemcf.calc_produce_sim()
    itemcf.evaluate()

    # 打印推荐的产品
    user = "A3AFHCR1S2HVQS"
    itemss = itemcf.recommend(user)
    print("用户 "+user+" 的推荐列表：")
    for i,j in itemss:
        print(itemcf.id2name[i],j)
