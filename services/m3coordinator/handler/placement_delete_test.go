package handler

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementDeleteHandler(t *testing.T) {
	mockClient, mockPlacementService := SetupPlacementTest(t)
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
