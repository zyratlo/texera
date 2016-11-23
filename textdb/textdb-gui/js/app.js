
var openMenu = function(){
	$('.menu').animate({
        'left': '0px'
    }, 200);
		
	$('.icon-menu').css({
		'visibility': 'hidden',
		'pointer': 'default'
	});
		
    $('body').animate({
        'left': '285px'
    }, 200);
};

var closeMenu = function(){
	$('.menu').animate({
		'left': '-285px'
	}, 200);
	
	$('.icon-menu').css({
		'visibility': 'visible',
		'pointer': 'pointer'
	});
	
	$('body').animate({
		'left': '0px'
	}, 200);
};

var main = function(){
	
    $('.icon-menu').click(openMenu);
	
    $('.icon-close').click(closeMenu);
	
	$('.menu ul li').on('click', function() {
		
		var panelToShow = $(this).attr('rel');
		var oldPanel = $('.panel.active').attr('id');
		
		$('li.active').removeClass('active');
		$('.panel.active').removeClass('active');
		
		if (oldPanel != panelToShow){
			$(this).addClass('active');
			$('#' + panelToShow).addClass('active');
		}		
	});
};

$(document).ready(main);
