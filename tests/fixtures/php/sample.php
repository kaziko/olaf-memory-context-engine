<?php

namespace MyPlugin;

use WP_Post;

class PostHandler {
    use Loggable;

    public function handle($post_id) {
        add_action('save_post', [$this, 'on_save']);
        return get_post($post_id);
    }

    public function on_save($post_id) {
        do_action('myPlugin_post_saved', $post_id);
    }
}

function bootstrap() {
    add_filter('the_content', 'process_content');
}
