video_url = 'https://www.youtube.com/watch?v=kYX87kkyubk'
preg_match("#(?<=v=)[a-zA-Z0-9-]+(?=&)|(?<=v\/)[^&\n]+(?=\?)|(?<=v=)[^&\n]+|(?<=youtu.be/)[^&\n]+#", video_url, matches);

// get video info from id
$video_id = $matches[0];
$video_info = file_get_contents('http://www.youtube.com/get_video_info?&video_id='.$video_id);
parse_str($video_info, $video_info_array);

if (isset($video_info_array['caption_tracks'])) {
    $tracks = explode(',', $video_info_array['caption_tracks']);

    // print info for each track (including url to track content)
    foreach ($tracks as $track) {
        parse_str($track, $output);
        print_r($output);
    }
}