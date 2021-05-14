import os
from collections import defaultdict
import pandas as pd
import supervisely_lib as sly
from supervisely_lib.video_annotation.key_id_map import KeyIdMap

my_app = sly.AppService()

TEAM_ID = int(os.environ['context.teamId'])
WORKSPACE_ID = int(os.environ['context.workspaceId'])
PROJECT_ID = int(os.environ['modal.state.slyProjectId'])
DATASET_ID = os.environ.get('modal.state.slyDatasetId', None)
if DATASET_ID is not None:
    DATASET_ID = int(DATASET_ID)

TOTAL = 'total'
COUNT_SUFFIX = '_cnt'
TAG_COLOMN = 'tag'
TAG_VALUE_COLOMN = 'tag_value'
FIRST_COLOMN = '#'


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
        row = [idx, name]
        if DATASET_ID is None:
            row.extend([0])
        for ds_name, ds_property_tags in datasets:
            row.extend([ds_property_tags[name]])
            if DATASET_ID is None:
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
                row_val = [idx, tag_name, str(val)]
                if DATASET_ID is None:
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


@my_app.callback("video_tag_stats")
@sly.timeit
def video_tag_stats(api: sly.Api, task_id, context, state, app_logger):

    project_info = api.project.get_info_by_id(PROJECT_ID)
    if project_info is None:
        raise RuntimeError("Project with ID {!r} not found".format(PROJECT_ID))
    if project_info.type != str(sly.ProjectType.VIDEOS):
        raise TypeError("Project type is {!r}, but have to be {!r}".format(project_info.type, sly.ProjectType.VIDEOS))

    meta_json = api.project.get_meta(PROJECT_ID)
    meta = sly.ProjectMeta.from_json(meta_json)
    if len(meta.tag_metas) == 0:
        app_logger.warn("Project {!r} have no tags".format(project_info.name))
        my_app.stop()

    columns = [FIRST_COLOMN, TAG_COLOMN]
    columns_for_values = [FIRST_COLOMN, TAG_COLOMN, TAG_VALUE_COLOMN]
    columns_frame_tag = [FIRST_COLOMN, TAG_COLOMN] #===========frame_tags=======
    columns_frame_tag_values = [FIRST_COLOMN, TAG_COLOMN, TAG_VALUE_COLOMN] #===========frame_tags=======
    columns_object_tag = [FIRST_COLOMN, TAG_COLOMN] #===========object_tags=======
    columns_object_tag_values = [FIRST_COLOMN, TAG_COLOMN, TAG_VALUE_COLOMN] #===========object_tags=======
    if DATASET_ID is None:
        columns.extend([TOTAL])
        columns_for_values.extend([TOTAL])
        columns_frame_tag.extend([TOTAL, TOTAL + COUNT_SUFFIX]) #===========frame_tags=======
        columns_frame_tag_values.extend([TOTAL]) #===========frame_tags=======
        columns_object_tag.extend([TOTAL])  # ===========object_tags=======
        columns_object_tag_values.extend([TOTAL])  # ===========object_tags=======

    datasets_counts = []
    datasets_values_counts = []
    datasets_frame_tag_counts = [] #===========frame_tags=======
    datasets_frame_tag_values_counts = [] #===========frame_tags=======
    datasets_object_tag_counts = []  # ===========object_tags=======
    datasets_object_tag_values_counts = []  # ===========object_tags=======

    key_id_map = KeyIdMap()

    for dataset in api.dataset.get_list(PROJECT_ID):
        if DATASET_ID is not None and dataset.id != DATASET_ID:
            continue

        columns.extend([dataset.name])
        ds_property_tags = defaultdict(int)

        columns_for_values.extend([dataset.name])
        ds_property_tags_values = defaultdict(lambda: defaultdict(int))

        #===========frame_tags=========================================
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
        progress = sly.Progress("Processing video tags ...", len(videos), app_logger)
        for batch in sly.batched(videos, batch_size=10):
            for video_info in batch:
                ann_info = api.video.annotation.download(video_info.id)
                ann = sly.VideoAnnotation.from_json(ann_info, meta, key_id_map)

                process_video_annotation(ann, ds_property_tags)
                process_video_annotation_tags_values(ann, ds_property_tags_values)

                process_video_ann_frame_tags(ann, ds_frame_tags, ds_frame_tags_counter) #===========frame_tags=======
                process_video_ann_frame_tags_vals(ann, ds_frame_tags_values) #===========frame_tags=======

                process_video_ann_object_tags(ann, ds_object_tags)  # ===========object_tags=======
                process_video_ann_object_tags_vals(ann, ds_object_tags_values)  # ===========object_tags=======

                progress.iter_done_report()

        datasets_counts.append((dataset.name, ds_property_tags))
        datasets_values_counts.append((dataset.name, ds_property_tags_values))
        datasets_frame_tag_counts.append((dataset.name, ds_frame_tags)) #===========frame_tags=======
        datasets_frame_tag_values_counts.append((dataset.name, ds_frame_tags_values)) #===========frame_tags=======
        datasets_object_tag_counts.append((dataset.name, ds_object_tags))  # ===========object_tags=======
        datasets_object_tag_values_counts.append((dataset.name, ds_object_tags_values))  # ===========object_tags=======

    #=========property_tags===============================================================
    df = get_pd_tag_stat(meta, datasets_counts, columns)
    print('Total video tags stats')
    print(df)
    #=========property_tags_values=========================================================
    df_values = get_pd_tag_values_stat(datasets_values_counts, columns_for_values)
    print('Total video tags values stats')
    print(df_values)

    # =========frame_tag=====================================================================
    data_frame_tags = []
    for idx, tag_meta in enumerate(meta.tag_metas):
        name = tag_meta.name
        row_frame_tags = [idx, name]
        if DATASET_ID is None:
            row_frame_tags.extend([0, 0])
        for ds_name, ds_frame_tags in datasets_frame_tag_counts:
            row_frame_tags.extend([ds_frame_tags[name], ds_frame_tags_counter[name]])
            if DATASET_ID is None:
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

    #==========object_tag================================================================
    df_object_tags = get_pd_tag_stat(meta, datasets_object_tag_counts, columns_object_tag)
    print('Total object tags stats')
    print(df_object_tags)
    # =========object_tags_values=========================================================
    df_object_values = get_pd_tag_values_stat(datasets_object_tag_values_counts, columns_object_tag_values)
    print('Total object tags values stats')
    print(df_object_values)

    my_app.stop()


def main():
    sly.logger.info("Script arguments", extra={
        "TEAM_ID": TEAM_ID,
        "WORKSPACE_ID": WORKSPACE_ID,
        "PROJECT_ID": PROJECT_ID
    })
    my_app.run(initial_events=[{"command": "video_tag_stats"}])


if __name__ == "__main__":
    sly.main_wrapper("main", main)