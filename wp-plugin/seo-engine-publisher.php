<?php
/**
 * Plugin Name: SEO Engine Publisher
 * Description: Connects WordPress to the SEO Engine platform for automated content publishing, schema management, and IndexNow key hosting.
 * Version: 1.0.0
 * Author: GetHubed
 */

defined('ABSPATH') || exit;

define('SEO_ENGINE_VERSION', '1.0.0');
define('SEO_ENGINE_OPTION_KEY', 'seo_engine_settings');

// --- REST API Registration ---
add_action('rest_api_init', function () {
    register_rest_route('seo-engine/v1', '/publish', [
        'methods'  => 'POST',
        'callback' => 'seo_engine_publish_post',
        'permission_callback' => 'seo_engine_verify_jwt',
    ]);
    register_rest_route('seo-engine/v1', '/status/(?P<post_id>\d+)', [
        'methods'  => 'GET',
        'callback' => 'seo_engine_get_status',
        'permission_callback' => 'seo_engine_verify_jwt',
    ]);
    register_rest_route('seo-engine/v1', '/health', [
        'methods'  => 'GET',
        'callback' => fn() => new WP_REST_Response(['status' => 'ok', 'version' => SEO_ENGINE_VERSION], 200),
        'permission_callback' => '__return_true',
    ]);
});

// --- JWT Verification ---
function seo_engine_verify_jwt(WP_REST_Request $request): bool {
    $options = get_option(SEO_ENGINE_OPTION_KEY, []);
    $secret  = $options['api_secret'] ?? '';
    if (!$secret) return false;
    $auth = $request->get_header('Authorization');
    if (!$auth || strpos($auth, 'Bearer ') !== 0) return false;
    $token = substr($auth, 7);
    return hash_equals($secret, $token);
}

// --- Publish Endpoint ---
function seo_engine_publish_post(WP_REST_Request $request): WP_REST_Response {
    $body = $request->get_json_params();
    $title   = sanitize_text_field($body['title'] ?? '');
    $content = wp_kses_post($body['content'] ?? '');
    $slug    = sanitize_title($body['slug'] ?? $title);
    $status  = in_array($body['status'] ?? 'publish', ['publish', 'draft']) ? $body['status'] : 'publish';
    $schema  = $body['schema_json'] ?? '';
    $canonical = esc_url_raw($body['canonical_url'] ?? '');

    if (empty($title) || empty($content)) {
        return new WP_REST_Response(['error' => 'title and content required'], 400);
    }

    // Inject schema into content if provided
    if ($schema) {
        $content = '<script type="application/ld+json">' . wp_json_encode(json_decode($schema, true)) . '</script>' . $content;
    }

    $post_data = ['post_title' => $title, 'post_content' => $content, 'post_status' => $status, 'post_name' => $slug, 'post_type' => 'post'];
    $existing = get_page_by_path($slug, OBJECT, 'post');
    if ($existing) {
        $post_data['ID'] = $existing->ID;
        $post_id = wp_update_post($post_data, true);
    } else {
        $post_id = wp_insert_post($post_data, true);
    }

    if (is_wp_error($post_id)) {
        return new WP_REST_Response(['error' => $post_id->get_error_message()], 500);
    }

    // Set canonical
    if ($canonical) {
        update_post_meta($post_id, '_seo_engine_canonical', $canonical);
        // Yoast compatibility
        if (defined('WPSEO_VERSION')) {
            update_post_meta($post_id, '_yoast_wpseo_canonical', $canonical);
        }
        // Rank Math compatibility
        update_post_meta($post_id, 'rank_math_canonical_url', $canonical);
    }

    return new WP_REST_Response(['post_id' => $post_id, 'url' => get_permalink($post_id), 'status' => $status], 200);
}

// --- Status Endpoint ---
function seo_engine_get_status(WP_REST_Request $request): WP_REST_Response {
    $post_id = (int) $request->get_param('post_id');
    $post = get_post($post_id);
    if (!$post) return new WP_REST_Response(['error' => 'not found'], 404);
    return new WP_REST_Response(['post_id' => $post_id, 'status' => $post->post_status, 'url' => get_permalink($post_id), 'modified' => $post->post_modified], 200);
}

// --- IndexNow Key Hosting ---
add_action('init', function () {
    $options = get_option(SEO_ENGINE_OPTION_KEY, []);
    $key     = $options['indexnow_key'] ?? '';
    if (!$key || !isset($_SERVER['REQUEST_URI'])) return;
    $uri = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
    if ($uri === "/{$key}.txt") {
        header('Content-Type: text/plain; charset=utf-8');
        echo $key;
        exit;
    }
});

// --- Admin Settings Page ---
add_action('admin_menu', function () {
    add_options_page('SEO Engine', 'SEO Engine', 'manage_options', 'seo-engine', 'seo_engine_settings_page');
});

function seo_engine_settings_page() {
    if (isset($_POST['seo_engine_save']) && check_admin_referer('seo_engine_settings')) {
        update_option(SEO_ENGINE_OPTION_KEY, [
            'api_secret'    => sanitize_text_field($_POST['api_secret'] ?? ''),
            'indexnow_key'  => sanitize_text_field($_POST['indexnow_key'] ?? ''),
            'platform_url'  => esc_url_raw($_POST['platform_url'] ?? ''),
        ]);
        echo '<div class="updated"><p>Settings saved.</p></div>';
    }
    $opts = get_option(SEO_ENGINE_OPTION_KEY, []);
    ?>
    <div class="wrap">
        <h1>SEO Engine Publisher Settings</h1>
        <form method="post">
            <?php wp_nonce_field('seo_engine_settings'); ?>
            <table class="form-table">
                <tr><th>API Secret</th><td><input type="password" name="api_secret" value="<?php echo esc_attr($opts['api_secret'] ?? ''); ?>" class="regular-text"></td></tr>
                <tr><th>IndexNow Key</th><td><input type="text" name="indexnow_key" value="<?php echo esc_attr($opts['indexnow_key'] ?? ''); ?>" class="regular-text"></td></tr>
                <tr><th>Platform URL</th><td><input type="url" name="platform_url" value="<?php echo esc_attr($opts['platform_url'] ?? ''); ?>" class="regular-text"></td></tr>
            </table>
            <p><input type="submit" name="seo_engine_save" class="button button-primary" value="Save Settings"></p>
        </form>
    </div>
    <?php
}

// --- Dashboard Widget ---
add_action('wp_dashboard_setup', function () {
    wp_add_dashboard_widget('seo_engine_widget', 'SEO Engine', function () {
        $options = get_option(SEO_ENGINE_OPTION_KEY, []);
        $platform = $options['platform_url'] ?? '#';
        $count = wp_count_posts()->publish;
        echo "<p>SEO Engine is active. <strong>{$count}</strong> posts published.</p>";
        echo "<p><a href='{$platform}' target='_blank'>Open Platform Dashboard &rarr;</a></p>";
    });
});
