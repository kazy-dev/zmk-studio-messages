/*
 * Live state: send the active layers, key presses and sensor rotations as ZMK Studio notifications
 * (keymap.live_state_changed) while the host keeps sending set_live_updates = true.
 *
 * Notifications are queued from the event listener and sent from a dedicated thread,
 * and dropped when the Studio TX buffer is short on space, so key processing never
 * blocks on a host that stopped reading. Every notification carries the full state,
 * so a dropped one is caught up by the next.
 *
 * SPDX-License-Identifier: MIT
 */

#include <zephyr/kernel.h>
#include <zephyr/logging/log.h>
#include <zephyr/spinlock.h>
#include <zephyr/sys/atomic.h>
#include <zephyr/sys/ring_buffer.h>
#include <zephyr/sys/util.h>

LOG_MODULE_DECLARE(zmk_studio, CONFIG_ZMK_STUDIO_LOG_LEVEL);

#include <zmk/event_manager.h>
#include <zmk/events/endpoint_changed.h>
#include <zmk/events/layer_state_changed.h>
#include <zmk/events/position_state_changed.h>
#include <zmk/events/sensor_event.h>
#include <zmk/keymap.h>
#include <zmk/studio/core.h>
#include <zmk/studio/rpc.h>

BUILD_ASSERT(CONFIG_ZMK_STUDIO_LIVE_STATE_MIN_TX_SPACE <= CONFIG_ZMK_STUDIO_RPC_TX_BUF_SIZE,
             "ZMK_STUDIO_LIVE_STATE_MIN_TX_SPACE must not exceed ZMK_STUDIO_RPC_TX_BUF_SIZE");

/* Same as max_count of pressed_positions in keymap.options.in */
#define LIVE_STATE_MAX_PRESSED 8

struct live_state_item {
    bool has_position;
    bool pressed;
    uint32_t position;
    bool has_sensor;
    bool sensor_clockwise;
    uint8_t sensor_index;
};

K_MSGQ_DEFINE(live_state_queue, sizeof(struct live_state_item),
              CONFIG_ZMK_STUDIO_LIVE_STATE_QUEUE_SIZE, 4);

static atomic_t live_enabled = ATOMIC_INIT(0);
static atomic_t last_refresh_seconds = ATOMIC_INIT(0);
/* Uptime (s) when sending started or Studio was last unlocked; used for re-locking */
static atomic_t enabled_since_seconds = ATOMIC_INIT(0);

/* Keys held now (shared by the listener and the sending thread) */
static struct k_spinlock pressed_lock;
static uint32_t pressed_positions[LIVE_STATE_MAX_PRESSED];
static size_t pressed_count;

static inline int64_t uptime_seconds(void) { return k_uptime_get() / MSEC_PER_SEC; }

static void live_state_disable(void) {
    if (atomic_cas(&live_enabled, 1, 0)) {
        LOG_DBG("Live state updates stopped");
        k_msgq_purge(&live_state_queue);
    }
}

static void live_state_track(uint32_t position, bool pressed) {
    k_spinlock_key_t key = k_spin_lock(&pressed_lock);
    size_t index = pressed_count;
    for (size_t i = 0; i < pressed_count; i++) {
        if (pressed_positions[i] == position) {
            index = i;
            break;
        }
    }
    if (pressed) {
        if (index == pressed_count && pressed_count < LIVE_STATE_MAX_PRESSED) {
            pressed_positions[pressed_count++] = position;
        }
    } else if (index < pressed_count) {
        pressed_positions[index] = pressed_positions[--pressed_count];
    }
    k_spin_unlock(&pressed_lock, key);
}

static void live_state_enqueue(const struct live_state_item *item) {
    if (!atomic_get(&live_enabled)) {
        return;
    }
    if (k_msgq_put(&live_state_queue, item, K_NO_WAIT) != 0) {
        LOG_DBG("Live state queue is full, dropping");
    }
}

#if IS_ENABLED(CONFIG_ZMK_STUDIO_LIVE_STATE_SENSORS)
/* 1: clockwise, -1: counter-clockwise, 0: no movement. Same rule as behavior_sensor_rotate_common.c */
static int sensor_direction(const struct zmk_sensor_event *ev) {
    if (ev->channel_data_size < 1) {
        return 0;
    }
    const struct sensor_value value = ev->channel_data[0].value;
    int32_t amount = value.val1 != 0 ? value.val1 : value.val2;
    return (amount > 0) - (amount < 0);
}
#endif

static void live_state_send(const struct live_state_item *item) {
    struct ring_buf *tx_buf = zmk_rpc_get_tx_buf();
    if (ring_buf_space_get(tx_buf) < CONFIG_ZMK_STUDIO_LIVE_STATE_MIN_TX_SPACE) {
        LOG_DBG("Live state: not enough TX space, dropping");
        return;
    }

    zmk_keymap_LiveState state = zmk_keymap_LiveState_init_zero;
    state.active_layer_ids = zmk_keymap_layer_state();
    state.highest_layer_index = zmk_keymap_highest_layer_active();
    state.default_layer_id = zmk_keymap_layer_default();
    state.has_position = item->has_position;
    state.position = item->position;
    state.pressed = item->pressed;
    state.has_sensor = item->has_sensor;
    state.sensor_index = item->sensor_index;
    state.sensor_clockwise = item->sensor_clockwise;

    k_spinlock_key_t key = k_spin_lock(&pressed_lock);
    size_t count = MIN(pressed_count, ARRAY_SIZE(state.pressed_positions));
    for (size_t i = 0; i < count; i++) {
        state.pressed_positions[i] = pressed_positions[i];
    }
    state.pressed_positions_count = count;
    k_spin_unlock(&pressed_lock, key);

    raise_zmk_studio_rpc_notification((struct zmk_studio_rpc_notification){
        .notification = ZMK_RPC_NOTIFICATION(keymap, live_state_changed, state)});
}

static void live_state_check_timeouts(void) {
    if (!atomic_get(&live_enabled)) {
        return;
    }
    int64_t now = uptime_seconds();

    if (now - (int64_t)atomic_get(&last_refresh_seconds) >
        CONFIG_ZMK_STUDIO_LIVE_STATE_TIMEOUT_SECONDS) {
        live_state_disable();
        return;
    }

#if defined(CONFIG_ZMK_STUDIO_LOCK_IDLE_TIMEOUT_SEC) && CONFIG_ZMK_STUDIO_LOCK_IDLE_TIMEOUT_SEC > 0
    /* Each refresh request resets Studio's idle lock timer, so apply the idle timeout here. */
    if (zmk_studio_core_get_lock_state() == ZMK_STUDIO_CORE_LOCK_STATE_UNLOCKED &&
        now - (int64_t)atomic_get(&enabled_since_seconds) > CONFIG_ZMK_STUDIO_LOCK_IDLE_TIMEOUT_SEC) {
        LOG_DBG("Live state: locking after the Studio idle timeout");
        zmk_studio_core_lock();
    }
#endif
}

static void live_state_main(void *p1, void *p2, void *p3) {
    ARG_UNUSED(p1);
    ARG_UNUSED(p2);
    ARG_UNUSED(p3);

    for (;;) {
        struct live_state_item item;
        k_timeout_t timeout = atomic_get(&live_enabled) ? K_SECONDS(1) : K_FOREVER;
        int ret = k_msgq_get(&live_state_queue, &item, timeout);

        live_state_check_timeouts();

        if (ret == 0 && atomic_get(&live_enabled)) {
            live_state_send(&item);
        }
    }
}

K_THREAD_DEFINE(live_state_thread, CONFIG_ZMK_STUDIO_LIVE_STATE_THREAD_STACK_SIZE,
                live_state_main, NULL, NULL, NULL, K_LOWEST_APPLICATION_THREAD_PRIO, 0, 0);

zmk_studio_Response set_live_updates(const zmk_studio_Request *req) {
    bool enable = req->subsystem.keymap.request_type.set_live_updates;

    if (enable) {
        atomic_val_t now = (atomic_val_t)uptime_seconds();
        atomic_set(&last_refresh_seconds, now);
        if (atomic_cas(&live_enabled, 0, 1)) {
            atomic_set(&enabled_since_seconds, now);
            LOG_DBG("Live state updates started");
        }
        live_state_enqueue(&(struct live_state_item){0});
    } else {
        live_state_disable();
    }

    return ZMK_RPC_RESPONSE(keymap, set_live_updates, enable);
}

/* Read-only, so allowed while locked */
ZMK_RPC_SUBSYSTEM_HANDLER(keymap, set_live_updates, ZMK_STUDIO_RPC_HANDLER_UNSECURED);

static int live_state_listener(const zmk_event_t *eh) {
    const struct zmk_position_state_changed *position = as_zmk_position_state_changed(eh);
    if (position) {
#if IS_ENABLED(CONFIG_ZMK_STUDIO_LIVE_STATE_KEY_POSITIONS)
        live_state_track(position->position, position->state);
        live_state_enqueue(&(struct live_state_item){
            .has_position = true, .position = position->position, .pressed = position->state});
#endif
        return ZMK_EV_EVENT_BUBBLE;
    }

#if IS_ENABLED(CONFIG_ZMK_STUDIO_LIVE_STATE_SENSORS)
    const struct zmk_sensor_event *sensor = as_zmk_sensor_event(eh);
    if (sensor) {
        int direction = sensor_direction(sensor);
        if (direction != 0) {
            live_state_enqueue(&(struct live_state_item){.has_sensor = true,
                                                         .sensor_index = sensor->sensor_index,
                                                         .sensor_clockwise = direction > 0});
        }
        return ZMK_EV_EVENT_BUBBLE;
    }
#endif

    if (as_zmk_layer_state_changed(eh)) {
        live_state_enqueue(&(struct live_state_item){0});
        return ZMK_EV_EVENT_BUBBLE;
    }

    const struct zmk_studio_core_lock_state_changed *lock = as_zmk_studio_core_lock_state_changed(eh);
    if (lock) {
        if (lock->state == ZMK_STUDIO_CORE_LOCK_STATE_UNLOCKED) {
            atomic_set(&enabled_since_seconds, (atomic_val_t)uptime_seconds());
        }
        return ZMK_EV_EVENT_BUBBLE;
    }

    /* The Studio transport follows the endpoint; the host has to ask again. */
    if (as_zmk_endpoint_changed(eh)) {
        live_state_disable();
    }

    return ZMK_EV_EVENT_BUBBLE;
}

ZMK_LISTENER(live_state, live_state_listener);
ZMK_SUBSCRIPTION(live_state, zmk_position_state_changed);
ZMK_SUBSCRIPTION(live_state, zmk_layer_state_changed);
ZMK_SUBSCRIPTION(live_state, zmk_endpoint_changed);
ZMK_SUBSCRIPTION(live_state, zmk_studio_core_lock_state_changed);
#if IS_ENABLED(CONFIG_ZMK_STUDIO_LIVE_STATE_SENSORS)
ZMK_SUBSCRIPTION(live_state, zmk_sensor_event);
#endif
