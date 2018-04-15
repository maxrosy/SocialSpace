import os


class Helper(object):

    ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

    def getRootPath(self):

        return os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    def convertIdtoMid(self,id):
        id = str(id)
        p1 = id[0:2]
        p2 = id[2:9]
        p3 = id[9:]
        s1 = self.__base62_encode(int(p1))
        s2 = self.__base62_encode(int(p2))
        s3 = self.__base62_encode(int(p3))
        if len(s2)<4:
            s2='0'+s2
        if len(s3)<4:
            s3='0'+s3
        return s1+s2+s3

    def convertMidtoId(self, mid):
        p1 = mid[0:1]
        p2 = mid[1:5]
        p3 = mid[5:]
        s1 = self.__base62_decode(p1)
        s2 = self.__base62_decode(p2)
        s3 = self.__base62_decode(p3)

        return int(s1+s2+s3)

    def __base62_encode(self, num, alphabet=ALPHABET):
        """Encode a number in Base X

        `num`: The number to encode
        `alphabet`: The alphabet to use for encoding
        """
        if (num == 0):
            return alphabet[0]
        arr = []
        base = len(alphabet)
        while num:
            rem = num % base
            num = num // base
            arr.append(alphabet[rem])
        arr.reverse()
        return ''.join(arr)

    def __base62_decode(self, string, alphabet=ALPHABET):
        """Decode a Base X encoded string into the number

        Arguments:
        - `string`: The encoded string
        - `alphabet`: The alphabet to use for encoding
        """
        base = len(alphabet)
        strlen = len(string)
        num = 0

        idx = 0
        for char in string:
            power = (strlen - (idx + 1))
            num += alphabet.index(char) * (base ** power)
            idx += 1

        return num
