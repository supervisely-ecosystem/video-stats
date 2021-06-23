import os
import supervisely_lib as sly
from supervisely_lib.video_annotation.key_id_map import KeyIdMap
import pandas as pd
import copy
from operator import add
from collections import defaultdict

my_app = sly.AppService()

TEAM_ID = int(os.environ['context.teamId'])
WORKSPACE_ID = int(os.environ['context.workspaceId'])
PROJECT_ID = int(os.environ["modal.state.slyProjectId"])
TASK_ID = int(os.environ["TASK_ID"])
OBJECTS = '_objects'
FIGURES = '_figures'
FRAMES = '_frames'
CLASS_NAME = 'class_name'
CLASSES = 'Classes'
TAGS = 'Tags'

TOTAL = 'total'
COUNT_SUFFIX = '_cnt'
TAG_COLOMN = 'tag'
TAG_VALUE_COLOMN = 'tag_value'

stat_types_str = os.environ['modal.state.currStat']

if stat_types_str == '[Classes]':
    stat_type = [CLASSES]
elif stat_types_str == '[Tags]':
    stat_type = [TAGS]
else:
    stat_type = [CLASSES, TAGS]


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


def process_video_annotation(ann, property_tags_counter):
    for tag in ann.tags:
        if tag.frame_range is None:
            property_tags_counter[tag.name] += 1


def process_video_annotation_tags_values(ann, property_tags_values_counter):
    for tag in ann.tags:
        if tag.frame_range is None:
            property_tags_values_counter[tag.name][tag.value] += 1


def process_video_ann_frame_tags(ann, frame_range_tags_counter, tags_counter):
    for tag in ann.tags:
        if tag.frame_range:
            number_of_frames = tag.frame_range[1] - tag.frame_range[0] + 1
            frame_range_tags_counter[tag.name] += number_of_frames
            tags_counter[tag.name] += 1


def process_video_ann_frame_tags_vals(ann, frame_range_tags_val_counter):
    for tag in ann.tags:
        if tag.frame_range:
            number_of_frames = tag.frame_range[1] - tag.frame_range[0] + 1
            frame_range_tags_val_counter[tag.name][tag.value] += number_of_frames


def process_video_ann_object_tags(ann, object_tags_counter):
    for curr_obj in ann.objects:
        for tag in curr_obj.tags:
            object_tags_counter[tag.name] += 1


def process_video_ann_object_tags_vals(ann, object_tags_val_counter):
    for curr_obj in ann.objects:
        for tag in curr_obj.tags:
            object_tags_val_counter[tag.name][tag.value] += 1


def get_pd_tag_stat(meta, datasets, columns):
    data = []
    for idx, tag_meta in enumerate(meta.tag_metas):
        name = tag_meta.name
        row = [name]
        row.extend([0])
        for ds_name, ds_property_tags in datasets:
            row.extend([ds_property_tags[name]])
            row[2] += ds_property_tags[name]
        data.append(row)

    df = pd.DataFrame(data, columns=columns)
    total_row = list(df.sum(axis=0))
    total_row[0] = len(df)
    total_row[1] = TOTAL
    df.loc[len(df)] = total_row

    return df


def get_pd_tag_values_stat(values_counts, columns):
    data_values = []
    idx = 0
    for ds_property_tags_values in values_counts:
        for tag_name, tag_vals in ds_property_tags_values[1].items():
            for val, cnt in tag_vals.items():
                row_val = [tag_name, str(val)]
                row_val.extend([0])
                row_val.extend([cnt])
                row_val[3] += cnt
                data_values.append(row_val)
                idx += 1
    df_values = pd.DataFrame(data_values, columns=columns)
    total_row = list(df_values.sum(axis=0))
    total_row[0] = len(df_values)
    total_row[1] = TOTAL
    total_row[2] = TOTAL
    df_values.loc[len(df_values)] = total_row

    return df_values


@my_app.callback("video_stats")
@sly.timeit
def video_stats(api: sly.Api, task_id, context, state, app_logger):

    project_info = api.project.get_info_by_id(PROJECT_ID)
    key_id_map = KeyIdMap()
    if project_info is None:
        raise RuntimeError("Project with ID {!r} not found".format(PROJECT_ID))
    if project_info.type != str(sly.ProjectType.VIDEOS):
        raise TypeError("Project type is {!r}, but have to be {!r}".format(project_info.type, sly.ProjectType.VIDEOS))

    meta_json = api.project.get_meta(project_info.id)
    meta = sly.ProjectMeta.from_json(meta_json)

    if len(meta.obj_classes) == 0 and CLASSES in stat_type:
        app_logger.warn("Project {!r} have no classes".format(project_info.name))

    if len(meta.tag_metas) == 0 and TAGS in stat_type:
        app_logger.warn("Project {!r} have no tags".format(project_info.name))

    if len(meta.obj_classes) == 0 and len(meta.tag_metas) == 0:
        app_logger.warn("Project {!r} have no classes and tags".format(project_info.name))
        my_app.stop()

    if CLASSES in stat_type:
        classes = []
        counter = {}
        for curr_class in meta.obj_classes:
            classes.append(curr_class.name)
            counter[curr_class.name] = 0

        columns_classes = [CLASS_NAME, 'total_objects', 'total_figures', 'total_frames']
        data = {CLASS_NAME: classes, 'total_objects': [0] * len(classes), 'total_figures': [0] * len(classes), 'total_frames': [0] * len(classes)}

    if TAGS in stat_type:
        columns = [TAG_COLOMN]
        columns_for_values = [TAG_COLOMN, TAG_VALUE_COLOMN]
        columns_frame_tag = [TAG_COLOMN]  # ===========frame_tags=======
        columns_frame_tag_values = [TAG_COLOMN, TAG_VALUE_COLOMN]  # ===========frame_tags=======
        columns_object_tag = [TAG_COLOMN]  # ===========object_tags=======
        columns_object_tag_values = [TAG_COLOMN, TAG_VALUE_COLOMN]  # ===========object_tags=======

        columns.extend([TOTAL])
        columns_for_values.extend([TOTAL])
        columns_frame_tag.extend([TOTAL, TOTAL + COUNT_SUFFIX])  # ===========frame_tags=======
        columns_frame_tag_values.extend([TOTAL])  # ===========frame_tags=======
        columns_object_tag.extend([TOTAL])  # ===========object_tags=======
        columns_object_tag_values.extend([TOTAL])  # ===========object_tags=======

        datasets_counts = []
        datasets_values_counts = []
        datasets_frame_tag_counts = []  # ===========frame_tags=======
        datasets_frame_tag_values_counts = []  # ===========frame_tags=======
        datasets_object_tag_counts = []  # ===========object_tags=======
        datasets_object_tag_values_counts = []  # ===========object_tags=======

    for dataset in api.dataset.get_list(PROJECT_ID):

        if CLASSES in stat_type:
            columns_classes.extend([dataset.name + OBJECTS, dataset.name + FIGURES, dataset.name + FRAMES])
            classes_counter = copy.deepcopy(counter)
            figures_counter = copy.deepcopy(counter)
            frames_counter = copy.deepcopy(counter)
            data[dataset.name + OBJECTS] = []
            data[dataset.name + FIGURES] = []
            data[dataset.name + FRAMES] = []
            videos = api.video.get_list(dataset.id)
            progress_classes = sly.Progress("Processing video classes ...", len(videos), app_logger)

        if TAGS in stat_type:
            columns.extend([dataset.name])
            ds_property_tags = defaultdict(int)

            columns_for_values.extend([dataset.name])
            ds_property_tags_values = defaultdict(lambda: defaultdict(int))

            # ===========frame_tags=========================================
            columns_frame_tag.extend([dataset.name, dataset.name + COUNT_SUFFIX])
            ds_frame_tags = defaultdict(int)
            ds_frame_tags_counter = defaultdict(int)

            columns_frame_tag_values.extend([dataset.name])
            ds_frame_tags_values = defaultdict(lambda: defaultdict(int))
            ds_frame_tags_values_counter = defaultdict(lambda: defaultdict(int))
            # ===========frame_tags=========================================

            # ===========object_tags=========================================
            columns_object_tag.extend([dataset.name])
            ds_object_tags = defaultdict(int)

            columns_object_tag_values.extend([dataset.name])
            ds_object_tags_values = defaultdict(lambda: defaultdict(int))
            # ===========object_tags=========================================

            videos = api.video.get_list(dataset.id)
            progress_tags = sly.Progress("Processing video tags ...", len(videos), app_logger)

        for batch in sly.batched(videos, batch_size=10):
            for video_info in batch:

                ann_info = api.video.annotation.download(video_info.id)
                ann = sly.VideoAnnotation.from_json(ann_info, meta, key_id_map)

                if CLASSES in stat_type:
                    classes_counter, figures_counter, frames_counter = items_counter(ann, classes_counter, figures_counter, frames_counter)
                    progress_classes.iter_done_report()

                if TAGS in stat_type:
                    process_video_annotation(ann, ds_property_tags)
                    process_video_annotation_tags_values(ann, ds_property_tags_values)

                    process_video_ann_frame_tags(ann, ds_frame_tags,
                                                 ds_frame_tags_counter)  # ===========frame_tags=======
                    process_video_ann_frame_tags_vals(ann, ds_frame_tags_values)  # ===========frame_tags=======

                    process_video_ann_object_tags(ann, ds_object_tags)  # ===========object_tags=======
                    process_video_ann_object_tags_vals(ann, ds_object_tags_values)  # ===========object_tags=======

                    progress_tags.iter_done_report()

        if CLASSES in stat_type:
            data = data_counter(data, dataset, classes, classes_counter, figures_counter, frames_counter)

        if TAGS in stat_type:
            datasets_counts.append((dataset.name, ds_property_tags))
            datasets_values_counts.append((dataset.name, ds_property_tags_values))
            datasets_frame_tag_counts.append((dataset.name, ds_frame_tags))  # ===========frame_tags=======
            datasets_frame_tag_values_counts.append(
                (dataset.name, ds_frame_tags_values))  # ===========frame_tags=======
            datasets_object_tag_counts.append((dataset.name, ds_object_tags))  # ===========object_tags=======
            datasets_object_tag_values_counts.append(
                (dataset.name, ds_object_tags_values))  # ===========object_tags=======

    if CLASSES in stat_type:
        classes.append('Total')
        for key, val in data.items():
            if key == CLASS_NAME:
                continue
            data[key].append(sum(val))
        df_classes = pd.DataFrame(data, columns=columns_classes, index=classes)
        print(df_classes)

    if TAGS in stat_type:
        # =========property_tags===============================================================
        df = get_pd_tag_stat(meta, datasets_counts, columns)
        print('Total video tags stats')
        print(df)
        # =========property_tags_values=========================================================
        df_values = get_pd_tag_values_stat(datasets_values_counts, columns_for_values)
        print('Total video tags values stats')
        print(df_values)

        # =========frame_tag=====================================================================
        data_frame_tags = []
        for idx, tag_meta in enumerate(meta.tag_metas):
            name = tag_meta.name
            row_frame_tags = [name]
            row_frame_tags.extend([0, 0])
            for ds_name, ds_frame_tags in datasets_frame_tag_counts:
                row_frame_tags.extend([ds_frame_tags[name], ds_frame_tags_counter[name]])
                row_frame_tags[2] += ds_frame_tags[name]
                row_frame_tags[3] += ds_frame_tags_counter[name]
            data_frame_tags.append(row_frame_tags)

        df_frame_tags = pd.DataFrame(data_frame_tags, columns=columns_frame_tag)
        total_row = list(df_frame_tags.sum(axis=0))
        total_row[0] = len(df_frame_tags)
        total_row[1] = TOTAL
        df_frame_tags.loc[len(df_frame_tags)] = total_row
        print('Total frame tags stats')
        print(df_frame_tags)

        # =========frame_tags_values=============================================================
        df_frame_tags_values = get_pd_tag_values_stat(datasets_frame_tag_values_counts, columns_frame_tag_values)
        print('Total frame tags values stats')
        print(df_frame_tags_values)

        # ==========object_tag================================================================
        df_object_tags = get_pd_tag_stat(meta, datasets_object_tag_counts, columns_object_tag)
        print('Total object tags stats')
        print(df_object_tags)
        # =========object_tags_values=========================================================
        df_object_values = get_pd_tag_values_stat(datasets_object_tag_values_counts, columns_object_tag_values)
        print('Total object tags values stats')
        print(df_object_values)


    user_image_table = {
        "columns": columns_classes,
        "data": list(data.values())
    }
    fields = []
    fields.extend([
        {"field": "data.userImageTable", "payload": user_image_table}])

    api.task.set_fields(task_id, fields)
    
    my_app.stop()


def main():
    sly.logger.info("Script arguments", extra={
        "TEAM_ID": TEAM_ID,
        "WORKSPACE_ID": WORKSPACE_ID,
        "PROJECT_ID": PROJECT_ID
    })

    data = {
        "userImageTable": {"columns": [], "data": []}
    }

    my_app.run(data=data, initial_events=[{"command": "video_stats"}])


if __name__ == "__main__":
    sly.main_wrapper("main", main)
