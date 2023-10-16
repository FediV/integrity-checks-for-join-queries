import json
import os
import re
import subprocess

import PySimpleGUI as sg

sg.theme('Reddit')
sg.set_options(font=("Noto Sans Balinese", 12))

NUMBER_OF_TUPLES_KEY = "numberOfTuples"
JOIN_PROPORTION_KEY = "joinProportion"
SEED_KEY = "seed"
GENERATE_NEW_DATA_KEY = "generateNewData"
RANDOM_PROBABILITY_KEY = "randomProbability"

NUMBER_OF_RUNS_KEY = "numberOfRuns"
LAZY_PROBABILITY_KEY = "lazyProbability"
TAMPERING_PROBABILITY_KEY = "tamperingProbability"
OVERWRITE_FILE_KEY = "overwriteStatisticsFile"

QUERY_INPUT_KEY = "queryFilter"
SEMI_JOIN_KEY = "isSemiJoin"
OCCURRENCES_KEY = "useOccurrences"
N_MIN_MARKER_KEY = "minNumberOfMarkers"
N_MAX_MARKER_KEY = "maxNumberOfMarkers"
N_MARKER_KEY = "numberOfMarkers"
TWIN_VALUES_KEY = "values"
P_TWIN_KEY = "ptwin"
NUMBER_WORKERS_KEY = "workers"
TRUSTED_KEY = "trustedWorkers"

client_params = [QUERY_INPUT_KEY, SEMI_JOIN_KEY, OCCURRENCES_KEY, N_MIN_MARKER_KEY, N_MAX_MARKER_KEY, N_MARKER_KEY,
                 TWIN_VALUES_KEY, P_TWIN_KEY, NUMBER_WORKERS_KEY]
db_params = [NUMBER_OF_TUPLES_KEY, JOIN_PROPORTION_KEY, SEED_KEY, GENERATE_NEW_DATA_KEY, RANDOM_PROBABILITY_KEY]
simulation_params = [NUMBER_OF_RUNS_KEY, OVERWRITE_FILE_KEY, LAZY_PROBABILITY_KEY, TAMPERING_PROBABILITY_KEY,
                     TRUSTED_KEY]

bool_params = [OCCURRENCES_KEY, GENERATE_NEW_DATA_KEY, OVERWRITE_FILE_KEY]
int_params = [N_MIN_MARKER_KEY, N_MAX_MARKER_KEY, N_MARKER_KEY, NUMBER_OF_TUPLES_KEY, SEED_KEY, NUMBER_OF_RUNS_KEY]
float_params = [P_TWIN_KEY, RANDOM_PROBABILITY_KEY, LAZY_PROBABILITY_KEY, TAMPERING_PROBABILITY_KEY, TRUSTED_KEY]
list_params = [TWIN_VALUES_KEY, NUMBER_WORKERS_KEY, JOIN_PROPORTION_KEY]


def update_json(json_data: dict, key_param: str, value_param, l, r):
    if isinstance(json_data, dict):
        if not r and not l:
            for key, value in json_data.items():
                if key == key_param:
                    if key_param == NUMBER_WORKERS_KEY:
                        json_data[key] = ["worker_0"] if value_param == "" or int(value_param) in [0, 1] \
                            else [f"worker_{i}" for i in range(int(value_param))]
                    else:
                        json_data[key] = int(value_param) if key_param in int_params \
                            else float(value_param) if key_param in float_params \
                            else list(map(float, value_param.split(","))) if key_param in list_params and key_param != "values" \
                    else list(map(lambda s: re.sub(r'[^a-zA-Z0-9\s_]', '', s).strip(), value_param.split(","))) if key_param in list_params \
                            else value_param
                    return
                update_json(value, key_param, value_param, l, r)
        elif r:
            for key, value in json_data.items():
                if key == 'R':
                    json_data['R'][key_param] = int(value_param) if key_param in int_params \
                        else float(value_param) if key_param in float_params \
                        else list(map(float, value_param.split(","))) if key_param in list_params and key_param != "values" \
                        else list(map(lambda s: re.sub(r'[^a-zA-Z0-9\s_]', '', s).strip(), value_param.split(","))) if key_param in list_params \
                        else value_param
                    return
                update_json(value, key_param, value_param, l, r)
        elif l:
            for key, value in json_data.items():
                if key == 'L':
                    json_data['L'][key_param] = int(value_param) if key_param in int_params \
                        else float(value_param) if key_param in float_params \
                        else list(map(float, value_param.split(","))) if key_param in list_params and key_param != "values" \
                        else list(map(lambda s: re.sub(r'[^a-zA-Z0-9\s_]', '', s).strip(), value_param.split(","))) if key_param in list_params \
                        else value_param
                    return
                update_json(value, key_param, value_param, l, r)


def write_file(filename, new_params):
    with open('../' + filename) as file:
        data = json.load(file)
    for k, v in new_params.items():
        key_to_update = k.split('_')[0]
        if key_to_update == NUMBER_WORKERS_KEY or v != "":
            update_json(data, key_to_update, v, k.find("_L") != -1, k.find("_R") != -1)
    with open('../' + filename, 'w') as file:
        json.dump(data, file, indent=4)


def save_state(values):
    with open('state.json', 'w') as file:
        json.dump(values, file, indent=2)


def load_state():
    try:
        with open('state.json', 'r') as file:
            try:
                return json.load(file)
            except:
                return None
    except FileNotFoundError:
        return None


def create_tab(i):
    global tab_count
    # Storage Server L
    title_L = sg.Text("Storage Server L", font=('Arial Bold', 14))
    query_label_L = sg.Text("Query: ", size=(18, 1))
    query_input_L = sg.Input("", key=QUERY_INPUT_KEY + "_L_sim_" + str(i), )
    number_of_tuples_label_L = sg.Text("Number of tuples: ", size=(18, 1))
    number_of_tuples_input_L = sg.Input("", key=NUMBER_OF_TUPLES_KEY + "_L_sim_" + str(i))
    join_proportion_label_L = sg.Text("Join proportion: ", size=(18, 1))
    join_proportion_input_L = sg.Input("", key=JOIN_PROPORTION_KEY + "_L_sim_" + str(i), expand_x=True)
    generate_new_data_label_L = sg.Text("Generate new data: ", size=(18, 1))
    generate_new_data_input_L = sg.Checkbox("", key=GENERATE_NEW_DATA_KEY + "_L_sim_" + str(i))
    random_probability_label_L = sg.Text("Random probability: ", size=(18, 1))
    random_probability_input_L = sg.Input("", key=RANDOM_PROBABILITY_KEY + "_L_sim_" + str(i))
    tab_layout_L = [
        [title_L],
        [query_label_L, query_input_L],
        [number_of_tuples_label_L, number_of_tuples_input_L],
        [join_proportion_label_L, join_proportion_input_L],
        [generate_new_data_label_L, generate_new_data_input_L],
        [random_probability_label_L, random_probability_input_L]
    ]
    # Storage Server R
    title_R = sg.Text("Storage Server R", font=('Arial Bold', 14))
    query_label_R = sg.Text("Query: ", size=(18, 1))
    query_input_R = sg.Input("", key=QUERY_INPUT_KEY + "_R_sim_" + str(i))
    number_of_tuples_label_R = sg.Text("Number of tuples: ", size=(18, 1))
    number_of_tuples_input_R = sg.Input("", key=NUMBER_OF_TUPLES_KEY + "_R_sim_" + str(i))
    join_proportion_label_R = sg.Text("Join proportion: ", size=(18, 1))
    join_proportion_input_R = sg.Input("", key=JOIN_PROPORTION_KEY + "_R_sim_" + str(i), expand_x=True)
    generate_new_data_label_R = sg.Text("Generate new data: ", size=(18, 1))
    generate_new_data_input_R = sg.Checkbox("", key=GENERATE_NEW_DATA_KEY + "_R_sim_" + str(i))
    random_probability_label_R = sg.Text("Random probability: ", size=(18, 1))
    random_probability_input_R = sg.Input("", key=RANDOM_PROBABILITY_KEY + "_R_sim_" + str(i))
    tab_layout_R = [
        [title_R],
        [query_label_R, query_input_R],
        [number_of_tuples_label_R, number_of_tuples_input_R],
        [join_proportion_label_R, join_proportion_input_R],
        [generate_new_data_label_R, generate_new_data_input_R],
        [random_probability_label_R, random_probability_input_R]
    ]
    # Common params. column 1
    seed_label = sg.Text("Seed: ", size=(18, 1))
    seed_input = sg.Input("", key=SEED_KEY + "_sim_" + str(i))
    semi_join_label = sg.Text("Semi join: ", size=(18, 1))
    semi_join_input = sg.Checkbox("", key=SEMI_JOIN_KEY + "_sim_" + str(i))
    occurrences_label = sg.Text("Use occurrences: ", size=(18, 1))
    occurrences_input = sg.Checkbox("", key=OCCURRENCES_KEY + "_sim_" + str(i))
    number_of_workers_label = sg.Text("Number of workers: ", size=(18, 1))
    number_of_workers_input = sg.Input("", key=NUMBER_WORKERS_KEY + "_sim_" + str(i))
    trusted_percentage_label = sg.Text("Percentage of trusted: ", size=(18, 1))
    trusted_percentage_input = sg.Input("", key=TRUSTED_KEY + "_sim_" + str(i))
    column_layout_1 = [
        [seed_label, seed_input],
        [semi_join_label, semi_join_input],
        [occurrences_label, occurrences_input],
        [number_of_workers_label, number_of_workers_input],
        [trusted_percentage_label, trusted_percentage_input]
    ]
    # Common params. column 2
    number_of_runs_label = sg.Text("Number of runs: ", size=(18, 1))
    number_of_runs_input = sg.Input("", key=NUMBER_OF_RUNS_KEY + "_sim_" + str(i))
    lazy_probability_label = sg.Text("Lazy probability: ", size=(18, 1))
    lazy_probability_input = sg.Input("", key=LAZY_PROBABILITY_KEY + "_sim_" + str(i))
    tampering_probability_label = sg.Text("Tampering probability: ", size=(18, 1))
    tampering_probability_input = sg.Input("", key=TAMPERING_PROBABILITY_KEY + "_sim_" + str(i))
    overwrite_file_label = sg.Text("Overwrite statistic file: ", size=(18, 1))
    overwrite_file_input = sg.Checkbox("", key=OVERWRITE_FILE_KEY + "_sim_" + str(i))
    column_layout_2 = [
        [number_of_runs_label, number_of_runs_input],
        [lazy_probability_label, lazy_probability_input],
        [tampering_probability_label, tampering_probability_input],
        [overwrite_file_label, overwrite_file_input]
    ]
    # Control tuples params.
    title_control_tuples = sg.Text("Control tuples", font=('Arial Bold', 14))
    n_min_marker_label = sg.Text("Min number of markers: ", size=(18, 1))
    n_min_marker_input = sg.Input("", key=N_MIN_MARKER_KEY + "_sim_" + str(i))
    n_max_marker_label = sg.Text("Max number of markers: ", size=(18, 1))
    n_max_marker_input = sg.Input("", key=N_MAX_MARKER_KEY + "_sim_" + str(i))
    n_marker_label = sg.Text("Tot. number of markers: ", size=(18, 1))
    n_marker_input = sg.Input("", key=N_MARKER_KEY + "_sim_" + str(i))
    twin_values_label = sg.Text("Twin values: ", size=(18, 1))
    twin_values_input = sg.Input("", key=TWIN_VALUES_KEY + "_sim_" + str(i), expand_x=True)
    p_twin_label = sg.Text("Percentage of twins: ", size=(18, 1))
    p_twin_input = sg.Input("", key=P_TWIN_KEY + "_sim_" + str(i))
    control_tuples_layout = [
        [title_control_tuples],
        [n_min_marker_label, n_min_marker_input],
        [n_max_marker_label, n_max_marker_input],
        [n_marker_label, n_marker_input],
        [twin_values_label, twin_values_input],
        [p_twin_label, p_twin_input]
    ]

    tab = [
        [sg.Column(tab_layout_L), sg.VSeparator(), sg.Column(tab_layout_R)],
        [sg.HSeparator()],
        [sg.Column(column_layout_1), sg.Column(column_layout_2)],
        [sg.HSeparator()],
        [sg.Column(control_tuples_layout), sg.VSeparator(), ]
    ]
    return tab


if __name__ == "__main__":
    previous_state = load_state()
    tabs_id = [] if (previous_state is None or "tabs_id" not in previous_state.keys()) \
        else previous_state["tabs_id"]
    tab_count = 0 if (previous_state is None or "tab_count" not in previous_state.keys()) else \
        previous_state["tab_count"] - 2

    if len(tabs_id) == 0:
        tab_list = [
            [sg.Tab(f'Simulation - {i + 1}' if i <= tab_count else '+', create_tab(i + 1), key=f'Simulation - {i + 1}')
             for i in range(tab_count + 2)]]
    else:
        tab_list = [[sg.Tab(f'Simulation - {i}', create_tab(i), key=f'Simulation - {i}') for i in tabs_id],
                    [sg.Tab("+", create_tab(tab_count + 2), key=f'Simulation - {tab_count + 2}')]]

    if tab_count == 0:
        tabs_id = [1]

    tab_count += 2
    tab_group = sg.TabGroup(tab_list, key='tab_group')

    layout = [
        [tab_group],
        [sg.Button("Save"), sg.Cancel("Clear"), sg.Button("Remove"), sg.Push(),
         sg.Button("Run")]
    ]

    window = sg.Window('Simulation GUI', layout, finalize=True)
    window['tab_group'].bind('<<NotebookTabChanged>>', '+Switch')

    window.read(timeout=0)
    if previous_state is not None:
        for k, v in previous_state.items():
            if k in window.key_dict:
                window[k].update(v)

    while True:
        event, values = window.read()

        if event in (sg.WIN_CLOSED, 'Exit'):
            break
        if event == "Save":
            values["tabs_id"] = tabs_id
            values["tab_count"] = tab_count
            save_state(values)
        if event == "Clear":
            save_state({})

        if event == 'tab_group+Switch':
            i = int(values['tab_group'].split(' - ')[1])
            if i == tab_count:
                window[f'Simulation - {i}'].update(title=f'Simulation - {i}')
                new_plus_tab = sg.Tab('+', create_tab(i + 1), key=f'Simulation - {i + 1}')
                window['tab_group'].add_tab(new_plus_tab)
                tabs_id.append(i)
                tab_count += 1
            values["tabs_id"] = tabs_id
            values["tab_count"] = tab_count
            save_state(values)

        if event == "Remove":
            i = int(values['tab_group'].split(' - ')[1])
            window[f'Simulation - {i}'].update(visible=False)
            tabs_id.remove(i)
            values["tabs_id"] = tabs_id
            values["tab_count"] = tab_count
            save_state(values)
            prev_tabs = list(filter(lambda x: x < i, tabs_id))
            next_tabs = list(filter(lambda x: x > i, tabs_id))
            index_to_go = max(prev_tabs) if len(prev_tabs) != 0 \
                else min(next_tabs) if len(next_tabs) != 0 \
                else tab_count
            window[f'Simulation - {index_to_go}'].select()

        if event == "Run":
            tab_id = values['tab_group'].split(' - ')[1]
            client_new_params = {k.split("sim")[0]: v for k, v in values.items() if not isinstance(k, int)
                                 and k.split('_')[0] in client_params and k.split('_sim_')[1] == tab_id}
            db_new_params = {k.split("sim")[0]: v for k, v in values.items() if not isinstance(k, int)
                             and k.split('_')[0] in db_params and k.split('_sim_')[1] == tab_id}
            simulation_new_params = {k.split("sim")[0]: v for k, v in values.items() if not isinstance(k, int)
                                     and k.split('_')[0] in simulation_params and k.split('_sim_')[1] == tab_id}
            write_file("client.config.json", client_new_params)
            write_file("db.config.json", db_new_params)
            write_file("simulation.config.json", simulation_new_params)

            os.chdir("..")
            p = subprocess.Popen("java -cp Main.jar;ComputationalServer.jar;StorageServer.jar Main")
            if p.wait() == 0:
                exit(0)

    window.close()
