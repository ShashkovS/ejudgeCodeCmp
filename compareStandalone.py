# -*- coding: utf-8 -*-
import re
import os
import pickle
import json
import mariadb
import Levenshtein

EJUDGE_DIR = '/home/judges/'
EXTERNAL_XML_PATH = '/var/lib/ejudge/status/{}/dir/external.xml'
RUNS_DIR = '/var/archive/runs/'
RESULT_DIR = './'
MARIADB_USER = '*****'
MARIADB_PASSWORD = '*****'
INTERVAL = '365 DAY'  # where create_time >= SUBDATE(CURRENT_TIMESTAMP, INTERVAL {INTERVAL})

_STD = ['ArithmeticError', 'AssertionError', 'AttributeError', 'BaseException', 'BlockingIOError', 'BrokenPipeError', 'BufferError', 'BytesWarning',
        'ChildProcessError', 'ConnectionAbortedError', 'ConnectionError', 'ConnectionRefusedError', 'ConnectionResetError', 'DeprecationWarning', 'EOFError',
        'Ellipsis', 'EncodingWarning', 'EnvironmentError', 'Exception', 'False', 'FileExistsError', 'FileNotFoundError', 'FloatingPointError', 'FutureWarning',
        'GeneratorExit', 'IOError', 'ImportError', 'ImportWarning', 'IndentationError', 'IndexError', 'InterruptedError', 'IsADirectoryError', 'KeyError',
        'KeyboardInterrupt', 'LookupError', 'MemoryError', 'ModuleNotFoundError', 'NameError', 'None', 'NotADirectoryError', 'NotImplemented',
        'NotImplementedError', 'OSError', 'OverflowError', 'PendingDeprecationWarning', 'PermissionError', 'ProcessLookupError', 'RecursionError',
        'ReferenceError', 'ResourceWarning', 'RuntimeError', 'RuntimeWarning', 'StopAsyncIteration', 'StopIteration', 'SyntaxError', 'SyntaxWarning',
        'SystemError', 'SystemExit', 'TabError', 'TimeoutError', 'True', 'TypeError', 'UnboundLocalError', 'UnicodeDecodeError', 'UnicodeEncodeError',
        'UnicodeError', 'UnicodeTranslateError', 'UnicodeWarning', 'UserWarning', 'ValueError', 'Warning', 'WindowsError', 'ZeroDivisionError', '_',
        '_CHUNK_SIZE', '__abs__', '__add__', '__and__', '__bool__', '__build_class__', '__ceil__', '__class__', '__class_getitem__', '__contains__',
        '__debug__', '__del__', '__delattr__', '__delitem__', '__dict__', '__dir__', '__divmod__', '__doc__', '__enter__', '__eq__', '__exit__', '__float__',
        '__floor__', '__floordiv__', '__format__', '__ge__', '__getattribute__', '__getformat__', '__getitem__', '__getnewargs__', '__gt__', '__hash__',
        '__iadd__', '__iand__', '__import__', '__imul__', '__index__', '__init__', '__init_subclass__', '__int__', '__invert__', '__ior__', '__isub__',
        '__iter__', '__ixor__', '__le__', '__len__', '__loader__', '__lshift__', '__lt__', '__mod__', '__mul__', '__name__', '__ne__', '__neg__', '__new__',
        '__next__', '__or__', '__package__', '__pos__', '__pow__', '__radd__', '__rand__', '__rdivmod__', '__reduce__', '__reduce_ex__', '__repr__',
        '__reversed__', '__rfloordiv__', '__rlshift__', '__rmod__', '__rmul__', '__ror__', '__round__', '__rpow__', '__rrshift__', '__rshift__', '__rsub__',
        '__rtruediv__', '__rxor__', '__set_format__', '__setattr__', '__setitem__', '__sizeof__', '__spec__', '__str__', '__sub__', '__subclasshook__',
        '__truediv__', '__trunc__', '__xor__', '_checkClosed', '_checkReadable', '_checkSeekable', '_checkWritable', '_finalizing', 'abs', 'acos', 'acosh',
        'add', 'aiter', 'all', 'and', 'anext', 'any', 'append', 'as', 'as_integer_ratio', 'ascii', 'asin', 'asinh', 'assert', 'async', 'atan', 'atan2', 'atanh',
        'await', 'bin', 'bit_count', 'bit_length', 'bool', 'break', 'breakpoint', 'buffer', 'bytearray', 'bytes', 'callable', 'capitalize', 'casefold', 'ceil',
        'center', 'chr', 'class', 'classmethod', 'clear', 'close', 'closed', 'comb', 'compile', 'complex', 'conjugate', 'continue', 'copy', 'copyright',
        'copysign', 'cos', 'cosh', 'count', 'credits', 'def', 'degrees', 'del', 'delattr', 'denominator', 'detach', 'dict', 'difference', 'difference_update',
        'dir', 'discard', 'dist', 'divmod', 'e', 'elif', 'else', 'encode', 'encoding', 'endswith', 'enumerate', 'erf', 'erfc', 'errors', 'eval', 'except',
        'exec', 'execfile', 'exit', 'exp', 'expandtabs', 'expm1', 'extend', 'fabs', 'factorial', 'fileno', 'filter', 'finally', 'find', 'float', 'floor',
        'flush', 'fmod', 'for', 'format', 'format_map', 'frexp', 'from', 'from_bytes', 'fromhex', 'fromkeys', 'frozenset', 'fsum', 'gamma', 'gcd', 'get',
        'getattr', 'global', 'globals', 'hasattr', 'hash', 'help', 'hex', 'hypot', 'id', 'if', 'imag', 'import', 'in', 'index', 'inf', 'input', 'insert', 'int',
        'intersection', 'intersection_update', 'is', 'is_integer', 'isalnum', 'isalpha', 'isascii', 'isatty', 'isclose', 'isdecimal', 'isdigit', 'isdisjoint',
        'isfinite', 'isidentifier', 'isinf', 'isinstance', 'islower', 'isnan', 'isnumeric', 'isprintable', 'isqrt', 'isspace', 'issubclass', 'issubset',
        'issuperset', 'istitle', 'isupper', 'items', 'iter', 'join', 'keys', 'lambda', 'lcm', 'ldexp', 'len', 'lgamma', 'license', 'line_buffering', 'list',
        'ljust', 'locals', 'log', 'log10', 'log1p', 'log2', 'lower', 'lstrip', 'maketrans', 'map', 'max', 'memoryview', 'min', 'mode', 'modf', 'name', 'nan',
        'newlines', 'next', 'nextafter', 'nonlocal', 'not', 'numerator', 'object', 'oct', 'open', 'or', 'ord', 'partition', 'pass', 'perm', 'pi', 'pop',
        'popitem', 'pow', 'print', 'prod', 'property', 'quit', 'radians', 'raise', 'range', 'read', 'readable', 'readline', 'readlines', 'real', 'reconfigure',
        'remainder', 'remove', 'removeprefix', 'removesuffix', 'replace', 'repr', 'return', 'reverse', 'reversed', 'rfind', 'rindex', 'rjust', 'round',
        'rpartition', 'rsplit', 'rstrip', 'runfile', 'seek', 'seekable', 'set', 'setattr', 'setdefault', 'sin', 'sinh', 'slice', 'sort', 'sorted', 'split',
        'splitlines', 'sqrt', 'startswith', 'staticmethod', 'str', 'strip', 'sum', 'super', 'swapcase', 'symmetric_difference', 'symmetric_difference_update',
        'tan', 'tanh', 'tau', 'tell', 'title', 'to_bytes', 'translate', 'trunc', 'truncate', 'try', 'tuple', 'type', 'ulp', 'union', 'update', 'upper',
        'values', 'vars', 'while', 'with', 'writable', 'write', 'write_through', 'writelines', 'yield', 'zfill', 'zip']

START_SYMBOL = '\u2794'
INDENT_SYMBOL = chr(ord(START_SYMBOL) + 1)
_STDD = {name: chr(i) for i, name in enumerate(_STD, start=ord(START_SYMBOL) + 10)}
G = ''.join(_STDD.values())
assert re.findall(r'\w', G) == []


def prepare_source0(src):
    """Здесь мы готовим исходник. Убираем лишние пробелы, сглаживаем имена переменных, делаем одинаковые кавычки"""
    src = re.sub(r'\s*#.*', '', src)  # Удаляем комментарии
    src = src.strip()  # Убираем все пробелы в начале программы и в конце
    src = re.sub(r'\s+\n', r'\n', src)  # Убираем все пробелы в конце строк
    src = re.sub(r'\'', r'"', src)  # Единый формат кавычек
    src = re.sub(r'\n{2,}', r'\n', src)  # Убираем пустые строки
    return src


def rep_word(match):
    return _STDD.get(match.group(), START_SYMBOL)


def rep_space(match):
    return INDENT_SYMBOL * (len(match.group()) // 4)


def prepare_source1(src):
    # Заменяем имена переменных на START_SYMBOL, а питоновские команды на однобуквенные заменители
    src = re.sub(r'\b[a-zA-Z_][a-zA-Z_0-9]*\b', rep_word, src)
    src = re.sub(r'(?<=\n)(    |\t)+', rep_space, src)
    src = src.replace(' ', '')  # Убираем вообще все оставшиеся пробелы
    return src


def cmp(src1, src2):
    return Levenshtein.ratio(prepare_source1(prepare_source0(src1)), prepare_source1(prepare_source0(src2)))


def ratio(src1, src2):
    return Levenshtein.ratio(src1, src2) ** 4


def get_contest_updates():
    conn_params = {
        "user": MARIADB_USER,
        "password": MARIADB_PASSWORD,
        "host": "localhost",
        "database": "ejudge"
    }
    connection = mariadb.connect(**conn_params)
    cursor = connection.cursor()
    cursor.execute(f'''
        select distinct contest_id from runs where create_time >= SUBDATE(CURRENT_TIMESTAMP, INTERVAL {INTERVAL});
    ''')
    contests = [f'{int(row[0]):06}' for row in cursor.fetchall()]
    #    contests = ['001491']
    return contests


class Problem():
    def __init__(self, problem_num, problem_name):
        self.prob_id = problem_num
        self.name = problem_name

    def __str__(self):
        return 'Problem ID = "{}", problem name = "{}";'.format(self.prob_id, self.name)


class Source():
    def __init__(self, run_id, src):
        self.run_id = run_id
        self.src = prepare_source0(src)
        self.prep_src = prepare_source1(self.src)

    def __str__(self):
        return 'Run ID = "{}", Source = \n{}\n\nPrepared source = \n{}'.format(self.run_id, self.src, self.prep_src)


class Comparison():
    def __init__(self, problem, student1, student2, run_id1, run_id2, src1, src2):
        self.problem = problem
        self.student1 = student1
        self.student2 = student2
        self.run_id1 = run_id1
        self.run_id2 = run_id2
        self.len1 = len(src1.prep_src)
        self.len2 = len(src2.prep_src)
        self.match = ratio(src1.prep_src, src2.prep_src)
        # use_len = min(self.len1, self.len2) + abs(self.len1 - self.len2) // 4
        # pwr = (100 / max(100, use_len)) ** 0.5
        # self.score = ((self.match + 1) / (use_len + 1)) ** pwr
        self.score = self.match

    def __str__(self):
        return 'Problem = {}, Student1 = {}, Student2 = {}, Run ID1 = "{}", Run ID2 = "{}", Len1 = {}, Len2 = {}, Distance = {}, Score = {};' \
            .format(self.problem, self.student1, self.student2, self.run_id1, self.run_id2, self.len1, self.len2, self.match, self.score)


def parse_external_xml(contest):
    """ Читаем и парсим external.xml """
    # Загружаем содержимое external.xml
    ext = EXTERNAL_XML_PATH.format(contest)
    with open(ext,
              mode='r',
              encoding='utf-8') as ejudge_xml_file:
        external_xml_data = ejudge_xml_file.read()
    # Парсим
    students_dict = {m.group(1): m.group(2) for m in re.finditer(r'    \<user id="(.*?)" name="(.*?)"/\>', external_xml_data)}
    problems_dict = {m.group(1): Problem(m.group(2), m.group(3))
                     for m in re.finditer(r'    \<problem id="(.*?)" short_name="(.*?)" long_name="(.*?)"/\>', external_xml_data)}
    student_x_problem_to_last_run_dict = dict()
    for m in re.finditer(r'    \<run run_id="(.*?)" time=".*?" status="(.*?)" user_id="(.*?)" prob_id="(.*?)".*?/\>', external_xml_data):
        ##        print(m.group(3), m.group(4), m.group(1), m.group(2))
        if m.group(2) in ('OK', 'PR', 'RJ', 'DQ'):
            student_x_problem_to_last_run_dict[(m.group(3), m.group(4))] = m.group(1)
    # К данному моменту мы всё распарсили
    return students_dict, problems_dict, student_x_problem_to_last_run_dict


def retrieve_sources(student_x_problem_to_last_run_dict, contest):
    """ Получаем исходники всех последних run'ов по каждой задаче каждого школьника """
    # Загружаем координаты исходников
    runs_to_files_dict = {str(int(nm.split('.')[0])): os.path.join(top, nm).replace('\\', '/')
                          for top, dirs, files in os.walk(EJUDGE_DIR + contest + RUNS_DIR)
                          for nm in files}

    # Загружаем уже подготовленные исходники
    try:
        with open(RESULT_DIR + contest + '.sources.sav', 'rb') as source_savf:
            sources_dict = pickle.load(source_savf)
    except IOError:
        sources_dict = dict()
    except FileNotFoundError:
        sources_dict = dict()
    # Обновляем всё, что изменилось
    for (key) in student_x_problem_to_last_run_dict:
        if key not in sources_dict or sources_dict[key].run_id != student_x_problem_to_last_run_dict[key]:
            with open(runs_to_files_dict[student_x_problem_to_last_run_dict[key]], mode='r', encoding='utf-8') as source_file:
                try:
                    sources_dict[key] = Source(student_x_problem_to_last_run_dict[key], source_file.read())
                except UnicodeDecodeError:
                    with open(runs_to_files_dict[student_x_problem_to_last_run_dict[key]], mode='r', encoding='cp1251') as source_file1:
                        try:
                            sources_dict[key] = Source(student_x_problem_to_last_run_dict[key], source_file.read())
                        except:
                            print('Some problems with ' + runs_to_files_dict[student_x_problem_to_last_run_dict[key]])
                            sources_dict[key] = Source(student_x_problem_to_last_run_dict[key], '@')
                sources_dict[key].prep_src = prepare_source1(sources_dict[key].src)
    # Сохраняем всё на будущее для ускорения
    with open(RESULT_DIR + contest + '.sources.sav', 'wb') as source_savf:
        pickle.dump(sources_dict, source_savf, pickle.HIGHEST_PROTOCOL)
    return sources_dict


def compare_all_sources(student_x_problem_to_last_run_dict, students_dict, problems_dict):
    """ Делаем попарное сравнение исходников по одной и той же задаче для разных школьников """
    # Загружаем уже выполненные сравнения
    try:
        with open(RESULT_DIR + contest + '.comparisons.sav', 'rb') as comparisons_savf:
            comparisons_dict = pickle.load(comparisons_savf)
    except IOError:
        comparisons_dict = dict()
    except FileNotFoundError:
        comparisons_dict = dict()
    # Обновляем всё, что изменилось
    compared = 0
    for problem in problems_dict:
        print('Processing problem {}...'.format(problems_dict[problem].prob_id))
        for student1 in students_dict:
            for student2 in students_dict:
                if student1 < student2 and (student1, problem) in student_x_problem_to_last_run_dict and (
                        student2, problem) in student_x_problem_to_last_run_dict:
                    key = (problem, student1, student2)
                    if key not in comparisons_dict or comparisons_dict[key].run_id1 != student_x_problem_to_last_run_dict[(student1, problem)] \
                            or comparisons_dict[key].run_id2 != student_x_problem_to_last_run_dict[(student2, problem)]:
                        comparisons_dict[key] = Comparison(problem,
                                                           student1,
                                                           student2,
                                                           student_x_problem_to_last_run_dict[(student1, problem)],
                                                           student_x_problem_to_last_run_dict[(student2, problem)],
                                                           sources_dict[(student1, problem)],
                                                           sources_dict[(student2, problem)])
                        compared += 1
    if compared > 0:
        print('We have just compared {} pairs of sources. Sorry, it was very slow...'.format(compared))
    # Сохраняем всё на будущее для ускорения
    with open(RESULT_DIR + contest + '.comparisons.sav', 'wb') as comparisons_savf:
        pickle.dump(comparisons_dict, comparisons_savf, pickle.HIGHEST_PROTOCOL)
    return comparisons_dict


def print_comparisons(comparisons_dict: dict, problems_dict, students_dict):
    problem_scores = {}
    for cmp in comparisons_dict.values():
        prob_id = problems_dict[cmp.problem].prob_id
        student1 = int(cmp.student1)
        student2 = int(cmp.student2)
        score = round(cmp.score, 4)
        if prob_id not in problem_scores:
            problem_scores[prob_id] = []
        problem_scores[prob_id].append([student1, student2, score])
    obj = {
        'students': {int(k): v for (k, v) in students_dict.items()},
        'problems': {p.prob_id: p.name for p in problems_dict.values()},
        'scores': problem_scores,
    }
    json.dump(obj, open(RESULT_DIR + contest + '.json', 'w', encoding='utf-8'), ensure_ascii=False)


def cluster_contest(problems_dict, comparisons_dict, min_identity, min_cluster):
    # Теперь анализируем граф расстояний
    clusters_dict = dict()
    for problem in problems_dict:
        edj_lst = []
        for key in comparisons_dict:
            cmp = comparisons_dict[key]
            if key[0] == problem and cmp.score >= min_identity:
                edj_lst.append([cmp.score, cmp.student1, cmp.student2])
        # Добавляем затравку --- кластер из школьников первого ребра
        if len(edj_lst) == 0:
            clusters_dict[problem] = set()
            continue
        total_set = {edj_lst[0][1], edj_lst[0][2]}
        cluster_lst = [total_set]
        for score, student1, student2 in edj_lst:
            if student1 not in total_set and student2 not in total_set:
                cluster_lst.append({student1, student2})
            else:
                in1 = -1
                in2 = -1
                for i in range(len(cluster_lst)):
                    if in1 == -1 and student1 in cluster_lst[i]:
                        in1 = i
                    elif in2 == -1 and student2 in cluster_lst[i]:
                        in2 = i
                if in1 == -1:
                    cluster_lst[in2] |= {student1}
                elif in2 == -1:
                    cluster_lst[in1] |= {student2}
                else:
                    cluster_lst[in1] != cluster_lst[in2]
                    del (cluster_lst[in2])
            total_set |= {student1, student2}
        for i in range(len(cluster_lst) - 1, -1, -1):
            if len(cluster_lst[i]) < min_cluster:
                del (cluster_lst[i])
        clusters_dict[problem] = set()
        for i in range(len(cluster_lst)):
            clusters_dict[problem] |= cluster_lst[i]
    return (clusters_dict)


MY_CONTESTS = get_contest_updates()
print(f'{MY_CONTESTS=}')

for contest in MY_CONTESTS:
    print("Processing contest {}...".format(contest))
    try:
        students_dict, problems_dict, student_x_problem_to_last_run_dict = parse_external_xml(contest)
    except Exception as e:
        print(e)
        pass

    print("Retrieving sources...")
    try:
        sources_dict = retrieve_sources(student_x_problem_to_last_run_dict, contest)
    except Exception as e:
        print(e)
        continue

    print("Starting comparison... (Will take some time)")
    comparisons_dict = compare_all_sources(student_x_problem_to_last_run_dict, students_dict, problems_dict)

    print("Contest {} is ready!".format(contest))

    print("Great! Printing results...")
    print_comparisons(comparisons_dict, problems_dict, students_dict)

print('Done!')
