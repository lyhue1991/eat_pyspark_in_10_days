#为了在RDD中使用自定义类，需要将类的创建代码其写入到一个文件中，否则会有序列化错误
class Student:
    def __init__(self,name,age,score):
        self.name = name
        self.age = age
        self.score = score
    def __gt__(self,other):
        if self.score > other.score:
            return True
        elif self.score==other.score and self.age>other.age:
            return True
        else:
            return False
