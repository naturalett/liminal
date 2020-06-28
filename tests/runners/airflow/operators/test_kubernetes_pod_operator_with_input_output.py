import unittest
from unittest import TestCase

from liminal.runners.airflow.operators.\
    kubernetes_pod_operator_with_input_output import _split_list


class TestSplitList(TestCase):
    def setUp(self) -> None:
        self.short_seq = [{f'task_{i}': f'value_{i}'} for i in range(3)]
        self.long_seq = [{f'task_{i}': f'value_{i}'} for i in range(10)]

    def test_seq_equal_num(self):
        num = len(self.short_seq)
        result = _split_list(self.short_seq, num)
        expected = [[{'task_0': 'value_0'}], [{'task_1': 'value_1'}],
                    [{'task_2': 'value_2'}]]
        self.assertListEqual(expected, result)

    def test_seq_grater_than_num(self):
        num = 3
        result = _split_list(self.long_seq, num)
        expected = [
            [{'task_0': 'value_0'},
             {'task_1': 'value_1'},
             {'task_2': 'value_2'}],
            [{'task_3': 'value_3'},
             {'task_4': 'value_4'},
             {'task_5': 'value_5'}],
            [{'task_6': 'value_6'},
             {'task_7': 'value_7'},
             {'task_8': 'value_8'},
             {'task_9': 'value_9'}]]

        self.assertEqual(expected, result)

    def test_seq_smaller_than_num(self):
        test_num_range = [8, 9, 10, 11, 12]
        for num in test_num_range:
            result = _split_list(self.short_seq, num)
            self.assertEqual(len(result), num)
            self.assertTrue(all([[i] in result for i in self.short_seq]))
            self.assertEqual([[]] * (num - len(self.short_seq)),
                             [i for i in result if i == []])

