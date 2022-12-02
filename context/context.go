// Copyright 2022 Molecula Corp (DBA FeatureBase). All rights reserved.
package context

import "context"

// Empty struct to avoid allocations
type contextKeyOriginalIP struct{}
type contextKeyRequestUserID struct{}
type contextKeyRequestRequestID struct{}

// OriginalIP gets the original IP from the context.
func OriginalIP(ctx context.Context) (originalIP string, ok bool) {
	originalIP, ok = ctx.Value(contextKeyOriginalIP{}).(string)
	return
}

// WithOriginalIP makes a new context with the originalIP in the context.
func WithOriginalIP(ctx context.Context, originalIP string) context.Context {
	return context.WithValue(ctx, contextKeyOriginalIP{}, originalIP)
}

func UserID(ctx context.Context) (userID string, ok bool) {
	userID, ok = ctx.Value(contextKeyRequestUserID{}).(string)
	return
}

func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, contextKeyRequestUserID{}, userID)
}

func RequestID(ctx context.Context) (userID string, ok bool) {
	userID, ok = ctx.Value(contextKeyRequestRequestID{}).(string)
	return
}

func WithRequestID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, contextKeyRequestRequestID{}, userID)
}
