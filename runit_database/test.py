class TestClass():
    STATIC_VAR = ''

    def __init__(self, *args, **kwargs):
        print(args)
        for key in kwargs.keys():
            self.__setattr__(key, kwargs[key])

    def newfunt(self):
        pass

    @staticmethod
    def statmet():
        pass

    @classmethod
    def clsmeth(cls):
        pass

class User(TestClass):
    def __init__(self, name, email):
        super().__init__(name, email)

if __name__ == '__main__':
    newTest = User(name='Amos Amissah', email='theonlyamos@gmail.com')
    while True:
        code = input('> ')
        exec(code)
