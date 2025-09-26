package host

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/activeactor"
	"github.com/italypaleale/francis/internal/eventqueue"
	components_mocks "github.com/italypaleale/francis/internal/mocks/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/testutil"
)

func TestGetAlarm(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	log := slog.New(slog.DiscardHandler)

	testData, _ := msgpack.Marshal("test-data")

	newHost := func() (*Host, *components_mocks.MockActorProvider) {
		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)

		// Create a minimal host for testing
		host := &Host{
			actorProvider: provider,
			actors:        haxmap.New[string, *activeactor.Instance](8),
			log:           log,
			clock:         clock,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					IdleTimeout: 5 * time.Minute,
				},
			},
			providerRequestTimeout: 30 * time.Second,
		}
		host.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeactor.Instance]{
			ExecuteFn: host.handleIdleActor,
			Clock:     clock,
		})
		return host, provider
	}

	t.Run("successful get alarm", func(t *testing.T) {
		host, provider := newHost()

		alarmRef := ref.NewAlarmRef("testactor", "actor1", "alarm1")
		dueTime := time.Now().Add(time.Hour)
		ttl := time.Now().Add(24 * time.Hour)

		// Set up provider expectations
		provider.
			On("GetAlarm", mock.MatchedBy(testutil.MatchContextInterface), alarmRef).
			Return(components.GetAlarmRes{
				AlarmProperties: ref.AlarmProperties{
					DueTime:  dueTime,
					Interval: "1h",
					Data:     testData,
					TTL:      &ttl,
				},
			}, nil).
			Once()

		// Test getting the alarm
		properties, err := host.GetAlarm(t.Context(), "testactor", "actor1", "alarm1")
		require.NoError(t, err)

		// Verify the properties
		assert.Equal(t, dueTime, properties.DueTime)
		assert.Equal(t, "1h", properties.Interval)
		assert.Equal(t, "test-data", properties.Data)
		assert.Equal(t, ttl, properties.TTL)

		// Assert expectations
		provider.AssertExpectations(t)
	})

	t.Run("get alarm without TTL", func(t *testing.T) {
		host, provider := newHost()

		alarmRef := ref.NewAlarmRef("testactor", "actor1", "alarm2")
		dueTime := time.Now().Add(time.Hour)

		// Set up provider expectations (no TTL)
		provider.
			On("GetAlarm", mock.MatchedBy(testutil.MatchContextInterface), alarmRef).
			Return(components.GetAlarmRes{
				AlarmProperties: ref.AlarmProperties{
					DueTime:  dueTime,
					Interval: "2h",
					Data:     testData,
					TTL:      nil,
				},
			}, nil).
			Once()

		// Test getting the alarm
		properties, err := host.GetAlarm(t.Context(), "testactor", "actor1", "alarm2")
		require.NoError(t, err)

		// Verify the properties
		assert.Equal(t, dueTime, properties.DueTime)
		assert.Equal(t, "2h", properties.Interval)
		assert.Equal(t, "test-data", properties.Data)
		assert.True(t, properties.TTL.IsZero())

		// Assert expectations
		provider.AssertExpectations(t)
	})

	t.Run("get alarm not found", func(t *testing.T) {
		host, provider := newHost()

		alarmRef := ref.NewAlarmRef("testactor", "actor1", "nonexistent")

		// Set up provider expectations
		provider.
			On("GetAlarm", mock.MatchedBy(testutil.MatchContextInterface), alarmRef).
			Return(components.GetAlarmRes{}, components.ErrNoAlarm).
			Once()

		// Test getting non-existent alarm
		properties, err := host.GetAlarm(t.Context(), "testactor", "actor1", "nonexistent")
		require.ErrorIs(t, err, actor.ErrAlarmNotFound)
		assert.Zero(t, properties)

		// Assert expectations
		provider.AssertExpectations(t)
	})

	t.Run("get alarm provider error", func(t *testing.T) {
		host, provider := newHost()

		alarmRef := ref.NewAlarmRef("testactor", "actor1", "alarm3")
		providerErr := errors.New("provider error")

		// Set up provider expectations
		provider.
			On("GetAlarm", mock.MatchedBy(testutil.MatchContextInterface), alarmRef).
			Return(components.GetAlarmRes{}, providerErr).
			Once()

		// Test getting alarm with provider error
		properties, err := host.GetAlarm(t.Context(), "testactor", "actor1", "alarm3")
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to get alarm")
		require.ErrorIs(t, err, providerErr)
		assert.Zero(t, properties)

		// Assert expectations
		provider.AssertExpectations(t)
	})
}

func TestSetAlarm(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	log := slog.New(slog.DiscardHandler)

	testData, _ := msgpack.Marshal("test-data")

	newHost := func() (*Host, *components_mocks.MockActorProvider) {
		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)

		// Create a minimal host for testing
		host := &Host{
			actorProvider: provider,
			actors:        haxmap.New[string, *activeactor.Instance](8),
			log:           log,
			clock:         clock,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					IdleTimeout: 5 * time.Minute,
				},
			},
			providerRequestTimeout: 30 * time.Second,
		}
		host.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeactor.Instance]{
			ExecuteFn: host.handleIdleActor,
			Clock:     clock,
		})
		return host, provider
	}

	t.Run("successful set alarm with TTL", func(t *testing.T) {
		host, provider := newHost()

		alarmRef := ref.NewAlarmRef("testactor", "actor1", "alarm1")
		dueTime := time.Now().Add(time.Hour)
		ttl := time.Now().Add(24 * time.Hour)

		properties := actor.AlarmProperties{
			DueTime:  dueTime,
			Interval: "1h",
			Data:     "test-data",
			TTL:      ttl,
		}

		expectedReq := components.SetAlarmReq{
			AlarmProperties: ref.AlarmProperties{
				DueTime:  dueTime,
				Interval: "1h",
				Data:     testData,
				TTL:      &ttl,
			},
		}

		// Set up provider expectations
		provider.
			On("SetAlarm", mock.MatchedBy(testutil.MatchContextInterface), alarmRef, expectedReq).
			Return(nil).
			Once()

		// Test setting the alarm
		err := host.SetAlarm(t.Context(), "testactor", "actor1", "alarm1", properties)
		require.NoError(t, err)

		// Assert expectations
		provider.AssertExpectations(t)
	})

	t.Run("successful set alarm without TTL", func(t *testing.T) {
		host, provider := newHost()

		alarmRef := ref.NewAlarmRef("testactor", "actor1", "alarm2")
		dueTime := time.Now().Add(time.Hour)

		properties := actor.AlarmProperties{
			DueTime:  dueTime,
			Interval: "2h",
			Data:     "test-data",
			// TTL is zero value (not set)
		}

		expectedReq := components.SetAlarmReq{
			AlarmProperties: ref.AlarmProperties{
				DueTime:  dueTime,
				Interval: "2h",
				Data:     testData,
			},
			// TTL should be nil when not set
		}

		// Set up provider expectations
		provider.
			On("SetAlarm", mock.MatchedBy(testutil.MatchContextInterface), alarmRef, expectedReq).
			Return(nil).
			Once()

		// Test setting the alarm
		err := host.SetAlarm(t.Context(), "testactor", "actor1", "alarm2", properties)
		require.NoError(t, err)

		// Assert expectations
		provider.AssertExpectations(t)
	})

	t.Run("set alarm with empty data", func(t *testing.T) {
		host, provider := newHost()

		alarmRef := ref.NewAlarmRef("testactor", "actor1", "alarm3")
		dueTime := time.Now().Add(time.Hour)

		properties := actor.AlarmProperties{
			DueTime:  dueTime,
			Interval: "30m",
			Data:     nil, // No data
		}

		expectedReq := components.SetAlarmReq{
			AlarmProperties: ref.AlarmProperties{
				DueTime:  dueTime,
				Interval: "30m",
				Data:     nil,
			},
		}

		// Set up provider expectations
		provider.
			On("SetAlarm", mock.MatchedBy(testutil.MatchContextInterface), alarmRef, expectedReq).
			Return(nil).
			Once()

		// Test setting the alarm
		err := host.SetAlarm(t.Context(), "testactor", "actor1", "alarm3", properties)
		require.NoError(t, err)

		// Assert expectations
		provider.AssertExpectations(t)
	})

	t.Run("set alarm provider error", func(t *testing.T) {
		host, provider := newHost()

		alarmRef := ref.NewAlarmRef("testactor", "actor1", "alarm4")
		dueTime := time.Now().Add(time.Hour)
		providerErr := errors.New("provider error")

		properties := actor.AlarmProperties{
			DueTime:  dueTime,
			Interval: "1h",
			Data:     "test-data",
		}

		expectedReq := components.SetAlarmReq{
			AlarmProperties: ref.AlarmProperties{
				DueTime:  dueTime,
				Interval: "1h",
				Data:     testData,
			},
		}

		// Set up provider expectations
		provider.
			On("SetAlarm", mock.MatchedBy(testutil.MatchContextInterface), alarmRef, expectedReq).
			Return(providerErr).
			Once()

		// Test setting alarm with provider error
		err := host.SetAlarm(t.Context(), "testactor", "actor1", "alarm4", properties)
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to set alarm")
		require.ErrorIs(t, err, providerErr)

		// Assert expectations
		provider.AssertExpectations(t)
	})
}

func TestDeleteAlarm(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	log := slog.New(slog.DiscardHandler)

	newHost := func() (*Host, *components_mocks.MockActorProvider) {
		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)

		// Create a minimal host for testing
		host := &Host{
			actorProvider: provider,
			actors:        haxmap.New[string, *activeactor.Instance](8),
			log:           log,
			clock:         clock,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					IdleTimeout: 5 * time.Minute,
				},
			},
			providerRequestTimeout: 30 * time.Second,
		}
		host.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeactor.Instance]{
			ExecuteFn: host.handleIdleActor,
			Clock:     clock,
		})
		return host, provider
	}

	t.Run("successful delete alarm", func(t *testing.T) {
		host, provider := newHost()

		alarmRef := ref.NewAlarmRef("testactor", "actor1", "alarm1")

		// Set up provider expectations
		provider.
			On("DeleteAlarm", mock.MatchedBy(testutil.MatchContextInterface), alarmRef).
			Return(nil).
			Once()

		// Test deleting the alarm
		err := host.DeleteAlarm(t.Context(), "testactor", "actor1", "alarm1")
		require.NoError(t, err)

		// Assert expectations
		provider.AssertExpectations(t)
	})

	t.Run("delete alarm not found", func(t *testing.T) {
		host, provider := newHost()

		alarmRef := ref.NewAlarmRef("testactor", "actor1", "nonexistent")

		// Set up provider expectations
		provider.
			On("DeleteAlarm", mock.MatchedBy(testutil.MatchContextInterface), alarmRef).
			Return(components.ErrNoAlarm).
			Once()

		// Test deleting non-existent alarm
		err := host.DeleteAlarm(t.Context(), "testactor", "actor1", "nonexistent")
		require.ErrorIs(t, err, actor.ErrAlarmNotFound)

		// Assert expectations
		provider.AssertExpectations(t)
	})

	t.Run("delete alarm provider error", func(t *testing.T) {
		host, provider := newHost()

		alarmRef := ref.NewAlarmRef("testactor", "actor1", "alarm2")
		providerErr := errors.New("provider error")

		// Set up provider expectations
		provider.
			On("DeleteAlarm", mock.MatchedBy(testutil.MatchContextInterface), alarmRef).
			Return(providerErr).
			Once()

		// Test deleting alarm with provider error
		err := host.DeleteAlarm(t.Context(), "testactor", "actor1", "alarm2")
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to delete alarm")
		require.ErrorIs(t, err, providerErr)

		// Assert expectations
		provider.AssertExpectations(t)
	})

	t.Run("delete multiple alarms", func(t *testing.T) {
		host, provider := newHost()

		alarmRef1 := ref.NewAlarmRef("testactor", "actor1", "alarm1")
		alarmRef2 := ref.NewAlarmRef("testactor", "actor1", "alarm2")
		alarmRef3 := ref.NewAlarmRef("testactor", "actor2", "alarm1")

		// Set up provider expectations for multiple alarms
		provider.
			On("DeleteAlarm", mock.MatchedBy(testutil.MatchContextInterface), alarmRef1).
			Return(nil).
			Once()
		provider.
			On("DeleteAlarm", mock.MatchedBy(testutil.MatchContextInterface), alarmRef2).
			Return(nil).
			Once()
		provider.
			On("DeleteAlarm", mock.MatchedBy(testutil.MatchContextInterface), alarmRef3).
			Return(nil).
			Once()

		// Test deleting multiple alarms
		err1 := host.DeleteAlarm(t.Context(), "testactor", "actor1", "alarm1")
		require.NoError(t, err1)

		err2 := host.DeleteAlarm(t.Context(), "testactor", "actor1", "alarm2")
		require.NoError(t, err2)

		err3 := host.DeleteAlarm(t.Context(), "testactor", "actor2", "alarm1")
		require.NoError(t, err3)

		// Assert expectations
		provider.AssertExpectations(t)
	})
}

func TestAlarmPropertiesConversion(t *testing.T) {
	t.Run("alarmPropertiesFromAlarmRes with TTL", func(t *testing.T) {
		dueTime := time.Now().Add(time.Hour)
		ttl := time.Now().Add(24 * time.Hour)

		res := components.GetAlarmRes{
			AlarmProperties: ref.AlarmProperties{
				DueTime:  dueTime,
				Interval: "1h",
				Data:     []byte("test-data"),
				TTL:      &ttl,
			},
		}

		properties, err := alarmPropertiesFromAlarmRes(res)
		require.NoError(t, err)

		assert.Equal(t, dueTime, properties.DueTime)
		assert.Equal(t, "1h", properties.Interval)
		assert.NotEmpty(t, properties.Data)
		assert.Equal(t, ttl, properties.TTL)
	})

	t.Run("alarmPropertiesFromAlarmRes without TTL", func(t *testing.T) {
		dueTime := time.Now().Add(time.Hour)

		res := components.GetAlarmRes{
			AlarmProperties: ref.AlarmProperties{
				DueTime:  dueTime,
				Interval: "2h",
				Data:     []byte("test-data-2"),
				TTL:      nil,
			},
		}

		properties, err := alarmPropertiesFromAlarmRes(res)
		require.NoError(t, err)

		assert.Equal(t, dueTime, properties.DueTime)
		assert.Equal(t, "2h", properties.Interval)
		assert.NotEmpty(t, properties.Data)
		assert.True(t, properties.TTL.IsZero())
	})

	t.Run("alarmPropertiesFromAlarmRes with invalid msgpack data", func(t *testing.T) {
		dueTime := time.Now().Add(time.Hour)
		// Intentionally invalid msgpack data
		invalidData := []byte{0xD8}
		res := components.GetAlarmRes{
			AlarmProperties: ref.AlarmProperties{
				DueTime:  dueTime,
				Interval: "1h",
				Data:     invalidData,
				TTL:      nil,
			},
		}
		_, err := alarmPropertiesFromAlarmRes(res)
		require.Error(t, err)
		require.ErrorContains(t, err, "msgpack")
	})

	t.Run("alarmPropertiesToAlarmReq with TTL", func(t *testing.T) {
		dueTime := time.Now().Add(time.Hour)
		ttl := time.Now().Add(24 * time.Hour)

		properties := actor.AlarmProperties{
			DueTime:  dueTime,
			Interval: "1h",
			Data:     []byte("test-data"),
			TTL:      ttl,
		}

		req, err := alarmPropertiesToAlarmReq(properties)
		require.NoError(t, err)

		assert.Equal(t, dueTime, req.DueTime)
		assert.Equal(t, "1h", req.Interval)
		assert.NotEmpty(t, req.Data)
		require.NotNil(t, req.TTL)
		assert.Equal(t, ttl, *req.TTL)
	})

	t.Run("alarmPropertiesToAlarmReq without TTL", func(t *testing.T) {
		dueTime := time.Now().Add(time.Hour)

		properties := actor.AlarmProperties{
			DueTime:  dueTime,
			Interval: "2h",
			Data:     []byte("test-data-2"),
			// TTL is zero value
		}

		req, err := alarmPropertiesToAlarmReq(properties)
		require.NoError(t, err)

		assert.Equal(t, dueTime, req.DueTime)
		assert.Equal(t, "2h", req.Interval)
		assert.NotEmpty(t, properties.Data)
		assert.Nil(t, req.TTL)
	})

	t.Run("alarmPropertiesToAlarmReq with unserializable data", func(t *testing.T) {
		dueTime := time.Now().Add(time.Hour)
		// Channels cannot be encoded by msgpack
		properties := actor.AlarmProperties{
			DueTime:  dueTime,
			Interval: "1h",
			Data:     make(chan int),
		}
		_, err := alarmPropertiesToAlarmReq(properties)
		require.Error(t, err)
		require.ErrorContains(t, err, "msgpack")
	})
}
