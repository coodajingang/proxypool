
class Utils(object):

    # 匿名类型  
    # 高匿  、 高匿名 、 普匿 、 透明 、 透明代理IP、 普通代理ip
    @classmethod
    def toAnoymousType(self, str):
        if str.find('高匿') > -1 or str.find('普匿') > -1:
            return 'A'
        elif str.find('普通') > -1:
            return 'C'
        elif str.find('透明') > -1:
            return 'T'
        return 'N'

    ### 统一将 http  https 转换为大写形式 
    @classmethod
    def tohttp(self, str):
        return str.upper()