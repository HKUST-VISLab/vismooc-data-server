from ..pipe import PipeModule

class FormatUserFile(PipeModule):

    def __init__(self):
        super().__init__()

    def process(self, raw_data):
        processed_data = super().process(raw_data)

        headers = processed_data['data'][0][:-1].split('\t')
        print(headers)
        print(len(headers))
        new_processed_data = []
        for row in processed_data['data'][1:]:
            row = row[:-1].split('\t')
            print(row)
            print(len(row))
            break
        processed_data['data'] = new_processed_data
        return processed_data

