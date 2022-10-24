// Copyright 2022 Molecula Corp (DBA FeatureBase). All rights reserved.
package authn

import "context"

// Empty struct to avoid allocations
type contextKeyAccessToken struct{}
type contextKeyRefreshToken struct{}
type contextKeyUserInfo struct{}
type contextKeyIndexes struct{}

// GetAccessToken gets the access token from a context.
func GetAccessToken(ctx context.Context) (token string, ok bool) {
	token, ok = ctx.Value(contextKeyAccessToken{}).(string)
	return
}

// WithAccessToken makes a new Context with an access token.
func WithAccessToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, contextKeyAccessToken{}, token)
}

// GetRefreshToken gets the refresh token from a context.
func GetRefreshToken(ctx context.Context) (token string, ok bool) {
	token, ok = ctx.Value(contextKeyRefreshToken{}).(string)
	return
}

// WithRefreshToken makes a new Context with a refresh token.
func WithRefreshToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, contextKeyRefreshToken{}, token)
}

// GetUserInfo gets the UserInfo from a context.
func GetUserInfo(ctx context.Context) (userInfo *UserInfo, ok bool) {
	userInfo, ok = ctx.Value(contextKeyUserInfo{}).(*UserInfo)
	return
}

// WithUserInfo makes a new Context with UserInfo.
func WithUserInfo(ctx context.Context, userInfo *UserInfo) context.Context {
	return context.WithValue(ctx, contextKeyUserInfo{}, userInfo)
}

// GetIndexes get the indices from a context.
func GetIndexes(ctx context.Context) (indexes []string, ok bool) {
	indexes, ok = ctx.Value(contextKeyIndexes{}).([]string)
	return
}

// WithIndexes makes a new Context with a []string containing the indicies.
func WithIndexes(ctx context.Context, indexes []string) context.Context {
	return context.WithValue(ctx, contextKeyUserInfo{}, indexes)
}
