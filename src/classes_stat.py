import os
import supervisely_lib as sly
from supervisely_lib.video_annotation.key_id_map import KeyIdMap
import pandas as pd
import copy
from operator import add

my_app = sly.AppService()

TEAM_ID = int(os.environ['context.teamId'])
WORKSPACE_ID = int(os.environ['context.workspaceId'])
PROJECT_ID = int(os.environ["modal.state.slyProjectId"])
OBJECTS = '_objects'
FIGURES = '_figures'
FRAMES = '_frames'


def items_counter(ann, classes_counter, figures_counter, frames_counter):
    for obj in ann.objects:
        classes_counter[obj.obj_class.name] += 1
    for figure in ann.figures:
        figures_counter[figure.video_object.obj_class.name] += 1
    for frame in ann.frames:
        already_on_frame = []
        for fig in frame.figures:
            if fig.video_object.obj_class.name not in already_on_frame:
                frames_counter[fig.video_object.obj_class.name] += 1
                already_on_frame.append(fig.video_object.obj_class.name)

    return classes_counter, figures_counter, frames_counter


def data_counter(data, dataset, classes, classes_counter, figures_counter, frames_counter):
    for class_name in classes:
        data[dataset.name + OBJECTS].append(classes_counter[class_name])
        data[dataset.name + '_figures'].append(figures_counter[class_name])
        data[dataset.name + '_frames'].append(frames_counter[class_name])
    data['total_objects'] = list(map(add, data['total_objects'], data[dataset.name + OBJECTS]))
    data['total_figures'] = list(map(add, data['total_figures'], data[dataset.name + FIGURES]))
    data['total_frames'] = list(map(add, data['total_frames'], data[dataset.name + FRAMES]))

    return data


@my_app.callback("video_stats")
@sly.timeit
def video_stats(api: sly.Api, task_id, context, state, app_logger):

    project_info = api.project.get_info_by_id(PROJECT_ID)
    if project_info is None:
        raise RuntimeError("Project with ID {!r} not found".format(PROJECT_ID))
    if project_info.type != str(sly.ProjectType.VIDEOS):
        raise TypeError("Project type is {!r}, but have to be {!r}".format(project_info.type, sly.ProjectType.VIDEOS))

    meta_json = api.project.get_meta(project_info.id)
    meta = sly.ProjectMeta.from_json(meta_json)
    if len(meta.obj_classes) == 0:
        app_logger.warn("Project {!r} have no classes".format(project_info.name))
        my_app.stop()

    classes = []
    counter = {}
    for curr_class in meta.obj_classes:
        classes.append(curr_class.name)
        counter[curr_class.name] = 0

    columns = ['total_objects', 'total_figures', 'total_frames']
    data = {'total_objects': [0] * len(classes), 'total_figures': [0] * len(classes), 'total_frames': [0] * len(classes)}

    key_id_map = KeyIdMap()
    for dataset in api.dataset.get_list(PROJECT_ID):
        columns.extend([dataset.name + OBJECTS, dataset.name + FIGURES, dataset.name + FRAMES])
        classes_counter = copy.deepcopy(counter)
        figures_counter = copy.deepcopy(counter)
        frames_counter = copy.deepcopy(counter)
        data[dataset.name + OBJECTS] = []
        data[dataset.name + FIGURES] = []
        data[dataset.name + FRAMES] = []
        videos = api.video.get_list(dataset.id)
        progress = sly.Progress("Processing video classes ...", len(videos), app_logger)
        for batch in sly.batched(videos, batch_size=10):
            for video_info in batch:
                ann_info = api.video.annotation.download(video_info.id)
                ann = sly.VideoAnnotation.from_json(ann_info, meta, key_id_map)
                classes_counter, figures_counter, frames_counter = items_counter(ann, classes_counter, figures_counter, frames_counter)
                progress.iter_done_report()

        data = data_counter(data, dataset, classes, classes_counter, figures_counter, frames_counter)

    classes.append('Total')
    for key, val in data.items():
        data[key].append(sum(val))
    df = pd.DataFrame(data, columns=columns, index=classes)
    print(df)

    my_app.stop()


def main():
    sly.logger.info("Script arguments", extra={
        "TEAM_ID": TEAM_ID,
        "WORKSPACE_ID": WORKSPACE_ID,
        "PROJECT_ID": PROJECT_ID
    })
    my_app.run(initial_events=[{"command": "video_stats"}])


if __name__ == "__main__":
    sly.main_wrapper("main", main)