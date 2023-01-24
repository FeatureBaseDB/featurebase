package dax

type Controller interface {
	Noder
	Schemar
}

// Ensure type implements interface.
var _ Noder = &nopController{}
var _ Schemar = &nopController{}

// nopController is a no-op implementation of the Controller interface.
type nopController struct {
	nopNoder
	NopSchemar
}

func NewNopController() *nopController {
	return &nopController{}
}
