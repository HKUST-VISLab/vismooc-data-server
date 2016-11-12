class Utility():
    """ This class just provide some tools"""
    @classmethod
    def split_comma(cls, csv_string):
        """split a string with comma"""
        tmp_stack = []
        results = []
        quota_number = 0
        for i, letter in enumerate(csv_string):
            if i == len(csv_string)-1:
                tmp_stack.append(letter)
                results.append("".join(tmp_stack))
                return results
            elif letter == "," and (quota_number == 2 or quota_number == 0):
                results.append("".join(tmp_stack))
                tmp_stack = []
                quota_number = 0
            else:
                tmp_stack.append(letter)
                if letter == "'" and csv_string[i-1] != "\\":
                    quota_number += 1
    @classmethod
    def remove_quotation_mark(cls, data):
        """remove quotation mark for a list of string"""
        if not isinstance(data, list):
            raise Exception("The type of data must be list")
        result = []
        for item in data:
            if item[0] == "'" and item[-1] == "'":
                item = item[1:-1]
            result.append(item)
        return result
