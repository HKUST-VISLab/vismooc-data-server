import re
from .pipe import PipeModule


class ProcessLogFile(PipeModule):

    def __init__(self):
        super().__init__()

    def process(self, raw_data):
        processed_data = super().process(raw_data)
        empty_username_pattern = r'"username"\s*:\s*"",'
        right_eventsource_pattern = r'"event_source"\s*:\s*"browser"'
        right_eventtype_pattern = r'"event_type"\s*:\s*"(hide_transcript|load_video|pause_video|play_video|seek_video|show_transcript|speed_change_video|stop_video|video_hide_cc_menu|video_show_cc_menu)"'
        page_property_pattern = r',?\s*"page"\s*:\s*"[^"]*"'
        agent_property_pattern = r',?\s*"agent"\s*:\s*"[^"]*"'
        session_property_pattern = r',?\s*"session"\s*:\s*"\w*"'
        eventsource_property_pattern = r',?\s*"event_source"\s*:\s*"browser"'

        re_search_right_eventsource_pattern = re.compile(
            right_eventsource_pattern)
        re_search_right_eventtype_pattern = re.compile(right_eventtype_pattern)
        re_filter_wrong_property_pattern = re.compile(
            "(" + empty_username_pattern + ")")
        re_replace_property_pattern = re.compile("(" + page_property_pattern + ")|(" + agent_property_pattern +
                                                 ")|(" + session_property_pattern + ")|(" +
                                                 eventsource_property_pattern + ")")

        new_processed_data = []
        for line in processed_data:
            #print(line)
            if re_filter_wrong_property_pattern.search(line) is None and re_search_right_eventsource_pattern.search(line) is not None and re_search_right_eventtype_pattern.search(line) is not None:
                #print(re_replace_property_pattern.sub("", line))
                new_processed_data.append(
                    re_replace_property_pattern.sub("", line))

        return new_processed_data
