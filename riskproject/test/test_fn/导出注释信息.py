#!/usr/bin/env python
# -*-coding:utf-8 -*-


"""
文档快速生成注释的方法介绍,首先我们要用到__all__属性
在Py中使用为导出__all__中的所有类、函数、变量成员等
在模块使用__all__属性可避免相互引用时命名冲突
"""
__all__ = ["Login", "check", "Shop", "upDateIt", "findIt", "deleteIt", "createIt"]


class Login:
    """
    测试注释一可以写上此类的作用说明等
    例如此方法用来写登录
    """

    def __init__(self):
        """
        初始化你要的参数说明
        那么登录可能要用到
        用户名username
        密码password
        """
        pass

    def check(self):
        """
        Data Preparation From CSV File & Insert into DB

        Parameters:
        :param parm_df: 交易流水df
        :param parm_label: 维度标签（手机号、身份证号、IP地址、设备指纹）

        Process:
        1.去掉空手机号的行记录
          Return: df_dropna
        2.根据“商户号+手机号”的维度去掉重复记录
          Return: df_dropdup
        3.找到一个商户号下，成功交易的手机号客户个数，此时的手机号已去重
          Return: mer_tot_lbl_df
        4.找到可疑手机号，如果1个手机号在2个或者2个以上的商户进行过交易，那么此手机号认为是可疑的手机号；
          Return: 可疑手机号列表（susp_phone_list）
        5.找到通过可疑手机号的交易流水
          Return: 可疑手机号流水（susp_phone_trx）
          交易流水中商户定义为待确认商户，即：用可疑手机号完成交易的商户
          Return: 待确认商户列表（pnd_mer_list）
        6.将待确认商户两两组合
          Return: 两两组合的商户列表（susp_mer_group_list）
        7.计算相似度指数（Tanimoto Coefficient)，得到待确认商户之间的相似度
          7.1 找到商户间手机号交易的交集
            a.将商户组里面的第1个商户去susp_phone_trx中找到此商户关联的手机号
              Return: a商户关联手机号列表（mer_a_phone_list）
            b.将商户组里面的第2个商户去susp_phone_trx中找到此商户关联的手机号
              Return: b商户关联手机号列表（mer_b_phone_list）
            c.计算：a商户和b商户手机号关联列表的交集手机号个数
              Return: inter_phone_cnt
          7.2 找到商户间手机号交易的并集
            a.将商户组里面的第1个商户去mer_tot_lbl_df中找到此商户关联的手机号个数
              Return: a商户关联手机号列表（mer_a_tot_phone_cnt）
            b.将商户组里面的第2个商户去mer_tot_lbl_df中找到此商户关联的手机号个数
              Return: b商户关联手机号列表（mer_b_tot_phone_cnt）
            c.计算：a商户和b商户手机号并集个数
              union_phone_cnt = mer_a_tot_phone_cnt + mer_b_tot_phone_cnt - inter_phone_cnt
              Return: union_phone_cnt
          7.3 计算相似度指数：TC
              Tanimoto Coefficient = inter_phone_cnt / union_phone_cnt
          8.汇总两两商户间的相似度指数
            Return: tc_rst

        Return:
        parm_df_dropna, tc_rst

        Raises:
            IOError: An error occurred accessing the bigtable.Table object.
        """
        pass


class Shop:
    """
    商品类所包含的属性及方法
    update改/更新
    find查找
    delete删除
    create添加
    """

    def __init__(self):
        """
        初始化商品的价格、日期、分类等
        """
        pass

    def upDateIt(self):
        """
        用来更新商品信息
        """
        pass

    def findIt(self):
        """
        查找商品信息
        """
        pass

    def deleteIt(self):
        """
        删除过期下架商品信息
        """
        pass

    def createIt(self):
        """
        创建新商品及上架信息
        """
        pass

if __name__=="__main__":
    print(help(导出注释信息))