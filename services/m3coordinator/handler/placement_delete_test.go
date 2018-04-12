package handler

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementDeleteHandler(t *testing.T) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := client.NewMockClient(ctrl)
	require.NotNil(t, mockClient)
	mockServices := services.NewMockServices(ctrl)
	require.NotNil(t, mockServices)
	mockPlacementService := placement.NewMockService(ctrl)
	require.NotNil(t, mockPlacementService)

	mockClient.EXPECT().Services(gomock.Not(nil)).Return(mockServices, nil).AnyTimes()
	mockServices.EXPECT().PlacementService(gomock.Not(nil), gomock.Not(nil)).Return(mockPlacementService, nil).AnyTimes()

	handler := NewPlacementDeleteHandler(mockClient)

	// Test delete success
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/placement/delete", nil)
	require.NotNil(t, req)
	mockPlacementService.EXPECT().Delete()
	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Test delete error
	w = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/placement/delete", nil)
	require.NotNil(t, req)
	mockPlacementService.EXPECT().Delete().Return(errors.New("error"))
	handler.ServeHTTP(w, req)

	resp = w.Result()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}
